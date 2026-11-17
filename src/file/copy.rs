use super::{File, Written};
use crate::Result;
use aws_sdk_s3::operation::copy_object::builders::CopyObjectFluentBuilder;
use futures_util::future::BoxFuture;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use std::future::IntoFuture;

/// Characters left as is in `x-amz-copy-source`: RFC 3986 unreserved characters and the `/` between key segments.
const COPY_SOURCE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// A server-side copy of one object, built by [`File::copy`].
///
/// - Fails with [`Error::NotFound`](crate::Error::NotFound) when the source does not exist.
/// - One copy carries at most 5 GB. This is also why there is no `rename`: it would be a copy plus a delete, and not
///   atomic.
/// - The request goes through the target's client, so both buckets must be reachable from it. Copies across regions
///   are out of scope.
#[must_use = "a request does nothing until it is awaited"]
pub struct CopyTo {
    request: CopyObjectFluentBuilder,
}

impl File {
    /// Copies this object to `target`, replacing any object there.
    pub fn copy(&self, target: &File) -> CopyTo {
        // The SDK sends `copy_source` as given, so the key is encoded here, exactly once (awslabs/aws-sdk-rust#1357).
        let source = format!(
            "{}/{}",
            self.storage.bucket(),
            utf8_percent_encode(&self.key, COPY_SOURCE)
        );
        let storage = &target.storage;
        let request = storage.client().copy_object().bucket(storage.bucket()).key(&target.key);

        CopyTo {
            request: request.copy_source(source),
        }
    }
}

impl CopyTo {
    /// Adjusts the SDK request for options this builder does not wrap, such as `metadata_directive`.
    pub fn with(mut self, adjust: impl FnOnce(CopyObjectFluentBuilder) -> CopyObjectFluentBuilder) -> Self {
        self.request = adjust(self.request);
        self
    }
}

impl IntoFuture for CopyTo {
    type Output = Result<Written>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let output = self.request.send().await?;

            Ok(Written {
                etag: output.copy_object_result.and_then(|result| result.e_tag),
                version: output.version_id,
            })
        })
    }
}
