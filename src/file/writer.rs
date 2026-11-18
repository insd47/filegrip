use super::{File, Written};
use crate::{Error, Result};
use aws_sdk_s3::operation::create_multipart_upload::builders::CreateMultipartUploadFluentBuilder;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::fmt;
use std::future::IntoFuture;

// S3 multipart limits: every part but the last is 5 MiB to 5 GiB, and an upload has at most 10,000 parts.
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
const MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
const MAX_PARTS: i32 = 10_000;

const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;

/// The start of a multipart upload, built by [`File::writer`].
///
/// Awaiting it starts the upload (`CreateMultipartUpload`) and resolves to a [`Writer`].
#[must_use = "a request does nothing until it is awaited"]
pub struct Upload {
    file: File,
    request: CreateMultipartUploadFluentBuilder,
    part_size: usize,
    concurrent: usize,
    absent: bool,
}

impl File {
    /// Uploads the object in parts, for bodies over 5 GB or of unknown size.
    pub fn writer(&self) -> Upload {
        let storage = &self.storage;
        let request = storage
            .client()
            .create_multipart_upload()
            .bucket(storage.bucket())
            .key(&self.key);

        Upload {
            file: self.clone(),
            request,
            part_size: DEFAULT_PART_SIZE,
            concurrent: 1,
            absent: false,
        }
    }
}

impl Upload {
    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.request = self.request.content_type(content_type);
        self
    }

    /// Adds a user-defined metadata entry, sent as `x-amz-meta-{key}`.
    ///
    /// - S3 stores the key in lowercase, and [`Stat::metadata`](super::Stat::metadata) returns it that way.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.request = self.request.metadata(key, value);
        self
    }

    /// Bytes per part, 8 MiB by default.
    ///
    /// - Must be between 5 MiB and 5 GiB, or awaiting fails with [`Error::Invalid`].
    /// - Parts are cut to exactly this size, as R2 requires. With at most 10,000 parts, the part size bounds the
    ///   object size: 8 MiB parts reach about 78 GiB.
    pub fn part_size(mut self, part_size: usize) -> Self {
        self.part_size = part_size;
        self
    }

    /// Parts in flight at once, 1 by default.
    ///
    /// - The writer holds up to `part_size * concurrent` bytes in memory.
    /// - Parts make progress only while [`Writer::write`] or [`Writer::close`] is being awaited, since the crate spawns
    ///   no tasks. A source that pauses between writes leaves queued parts idle, and a pause longer than the client's
    ///   timeouts fails them.
    pub fn concurrent(mut self, concurrent: usize) -> Self {
        self.concurrent = concurrent;
        self
    }

    /// Completes only when no object exists at the key; [`Writer::close`] fails with [`Error::AlreadyExists`]
    /// otherwise.
    pub fn if_absent(mut self) -> Self {
        self.absent = true;
        self
    }

    /// Adjusts the SDK request that starts the upload, for options this builder does not wrap.
    pub fn with(
        mut self,
        adjust: impl FnOnce(CreateMultipartUploadFluentBuilder) -> CreateMultipartUploadFluentBuilder,
    ) -> Self {
        self.request = adjust(self.request);
        self
    }
}

impl IntoFuture for Upload {
    type Output = Result<Writer>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let part_size = u64::try_from(self.part_size).unwrap_or(u64::MAX);

            if !(MIN_PART_SIZE..=MAX_PART_SIZE).contains(&part_size) {
                return Err(Error::invalid("the part size must be between 5 MiB and 5 GiB"));
            }

            if self.concurrent == 0 {
                return Err(Error::invalid("at least one part must be in flight"));
            }

            let output = self.request.send().await?;
            let upload_id = output
                .upload_id
                .ok_or_else(|| Error::unavailable("S3 returned no upload ID"))?;

            Ok(Writer {
                file: self.file,
                upload_id,
                part_size: self.part_size,
                concurrent: self.concurrent,
                absent: self.absent,
                buffer: BytesMut::new(),
                numbered: 0,
                parts: Vec::new(),
                pending: FuturesUnordered::new(),
            })
        })
    }
}

/// An open multipart upload.
///
/// - Finish it with [`close`](Writer::close) or [`abort`](Writer::abort). Dropping it leaves the uploaded parts in the
///   bucket, billed but invisible, because S3 cannot clean up on drop. A bucket lifecycle rule with
///   `AbortIncompleteMultipartUpload` removes those left behind by crashes.
/// - After a failed [`write`](Writer::write), call [`abort`](Writer::abort).
#[must_use = "an upload leaves its parts in the bucket until it is closed or aborted"]
pub struct Writer {
    file: File,
    upload_id: String,
    part_size: usize,
    concurrent: usize,
    absent: bool,
    buffer: BytesMut,
    numbered: i32,
    parts: Vec<CompletedPart>,
    pending: FuturesUnordered<BoxFuture<'static, Result<CompletedPart>>>,
}

impl Writer {
    /// Appends `bytes` to the object, uploading every part that fills up.
    pub async fn write(&mut self, bytes: impl Into<Bytes>) -> Result<()> {
        let mut bytes = bytes.into();

        while !bytes.is_empty() {
            // A chunk that covers a whole part goes out without copying.
            if self.buffer.is_empty() && bytes.len() >= self.part_size {
                let part = bytes.split_to(self.part_size);
                self.send(part).await?;
                continue;
            }

            let take = bytes.len().min(self.part_size - self.buffer.len());
            self.buffer.extend_from_slice(&bytes.split_to(take));

            if self.buffer.len() == self.part_size {
                let part = self.buffer.split().freeze();
                self.send(part).await?;
            }
        }

        Ok(())
    }

    /// Uploads the rest and completes the object.
    ///
    /// - If completing fails, the upload is aborted before the error returns, so nothing is left to clean up. A
    ///   conflicting conditional write (HTTP 409) needs the whole upload to be retried.
    pub async fn close(mut self) -> Result<Written> {
        let result = self.complete().await;

        if result.is_err() {
            // The completion error is the one the caller needs; parts an abort misses go to the lifecycle rule.
            let _ = self.abort_upload().await;
        }

        result
    }

    /// Cancels the upload and deletes the parts uploaded so far.
    pub async fn abort(mut self) -> Result<()> {
        self.abort_upload().await
    }

    async fn complete(&mut self) -> Result<Written> {
        // The last part may be smaller than the rest, and an upload needs at least one part, even an empty one.
        if !self.buffer.is_empty() || self.numbered == 0 {
            let part = self.buffer.split().freeze();
            self.send(part).await?;
        }

        while let Some(part) = self.pending.next().await {
            self.parts.push(part?);
        }

        self.parts.sort_by_key(|part| part.part_number);
        let parts = CompletedMultipartUpload::builder()
            .set_parts(Some(std::mem::take(&mut self.parts)))
            .build();
        let storage = &self.file.storage;
        let request = storage
            .client()
            .complete_multipart_upload()
            .bucket(storage.bucket())
            .key(&self.file.key);
        let mut request = request.upload_id(&self.upload_id).multipart_upload(parts);

        if self.absent {
            request = request.if_none_match("*");
        }

        let output = match request.send().await.map_err(Error::from) {
            Ok(output) => output,
            Err(error) if self.absent => return Err(error.absent()),
            Err(error) => return Err(error),
        };

        Ok(Written {
            etag: output.e_tag,
            version: output.version_id,
        })
    }

    async fn abort_upload(&mut self) -> Result<()> {
        self.pending.clear();

        let storage = &self.file.storage;
        let request = storage
            .client()
            .abort_multipart_upload()
            .bucket(storage.bucket())
            .key(&self.file.key);
        request.upload_id(&self.upload_id).send().await?;

        Ok(())
    }

    /// Starts uploading `part`, then waits until fewer than `concurrent` parts are in flight.
    async fn send(&mut self, part: Bytes) -> Result<()> {
        if self.numbered == MAX_PARTS {
            return Err(Error::invalid("the upload exceeds 10,000 parts; raise the part size"));
        }

        self.numbered += 1;
        let number = self.numbered;
        let storage = &self.file.storage;
        let request = storage
            .client()
            .upload_part()
            .bucket(storage.bucket())
            .key(&self.file.key);
        let request = request.upload_id(&self.upload_id).part_number(number).body(part.into());

        self.pending.push(Box::pin(async move {
            let output = request.send().await?;

            Ok(CompletedPart::builder()
                .set_e_tag(output.e_tag)
                .part_number(number)
                .build())
        }));

        while self.pending.len() >= self.concurrent {
            let Some(done) = self.pending.next().await else { break };
            self.parts.push(done?);
        }

        Ok(())
    }
}

impl fmt::Debug for Writer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Writer")
            .field("file", &self.file)
            .field("upload_id", &self.upload_id)
            .field("parts", &self.numbered)
            .finish_non_exhaustive()
    }
}
