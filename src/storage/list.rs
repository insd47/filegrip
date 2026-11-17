use super::Storage;
use crate::{Error, File, Result};
use aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder;
use aws_sdk_s3::types::Object;
use futures_util::stream::{self, Stream, StreamExt, TryStreamExt};
use std::time::SystemTime;

impl Storage {
    /// Every object whose key starts with `prefix`, in UTF-8 binary order of keys, across all pages.
    ///
    /// - Recursive: `videos/` also yields `videos/2026/a.mp4`. Use [`children`](Storage::children) for one level.
    /// - Without `s3:ListBucket`, fails with [`Error::Denied`].
    pub fn list(&self, prefix: impl Into<String>) -> impl Stream<Item = Result<Entry>> + Send + 'static {
        let storage = self.clone();
        let request = self.client.list_objects_v2().bucket(&self.bucket).prefix(prefix);

        pages(request)
            .map_ok(move |page| {
                let entries = page.contents.unwrap_or_default().into_iter();
                let entries = entries.filter_map(|object| Entry::from(&storage, object));

                stream::iter(entries.map(Ok).collect::<Vec<_>>())
            })
            .try_flatten()
    }

    /// The objects and prefixes one level below `prefix`, split at `/`, across all pages.
    ///
    /// - End `prefix` with `/`: `videos` also matches `videos-old/`, since keys are not normalized.
    /// - Each page yields its prefixes before its objects.
    /// - An empty folder made in the S3 console is a zero-byte object named like `videos/`, so it is a
    ///   [`Child::File`].
    pub fn children(&self, prefix: impl Into<String>) -> impl Stream<Item = Result<Child>> + Send + 'static {
        let storage = self.clone();
        let request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .delimiter("/");

        pages(request)
            .map_ok(move |page| {
                let prefixes = page.common_prefixes.unwrap_or_default().into_iter();
                let prefixes = prefixes.filter_map(|prefix| prefix.prefix).map(Child::Prefix);
                let files = page.contents.unwrap_or_default().into_iter();
                let files = files
                    .filter_map(|object| Entry::from(&storage, object))
                    .map(Child::File);

                stream::iter(prefixes.chain(files).map(Ok).collect::<Vec<_>>())
            })
            .try_flatten()
    }
}

/// One object found by [`Storage::list`] or [`Storage::children`]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Entry {
    pub file: File,
    /// Size in bytes
    pub size: u64,
    /// Entity tag, quoted as S3 returns it
    pub etag: Option<String>,
    pub modified: Option<SystemTime>,
}

impl Entry {
    fn from(storage: &Storage, object: Object) -> Option<Self> {
        Some(Self {
            file: storage.file(object.key?),
            size: object
                .size
                .and_then(|size| u64::try_from(size).ok())
                .unwrap_or_default(),
            etag: object.e_tag,
            modified: object.last_modified.and_then(|time| SystemTime::try_from(time).ok()),
        })
    }
}

/// One item one level below a prefix, found by [`Storage::children`]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Child {
    File(Entry),
    /// A common prefix ending in `/`, such as `videos/2026/`. Pass it to [`Storage::children`] to descend.
    Prefix(String),
}

/// The pages of a listing as a stream.
fn pages(
    request: ListObjectsV2FluentBuilder,
) -> impl Stream<Item = Result<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output>> + Send + 'static {
    let mut pages = request.into_paginator().send();

    stream::poll_fn(move |context| pages.poll_next(context)).map(|page| page.map_err(Error::from))
}
