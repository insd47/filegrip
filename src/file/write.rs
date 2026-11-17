use super::{File, presigning};
use crate::{Error, Result};
use aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder;
use aws_sdk_s3::presigning::PresignedRequest;
use aws_sdk_s3::primitives::ByteStream;
use futures_util::future::BoxFuture;
use std::future::IntoFuture;
use std::time::Duration;

/// A single-request upload of one object, built by [`File::write`].
///
/// - Replaces any object at the key unless [`if_absent`](Write::if_absent) is set.
/// - One request carries at most 5 GB. Use [`File::writer`] for larger or streamed bodies.
#[must_use = "a request does nothing until it is awaited"]
pub struct Write {
    request: PutObjectFluentBuilder,
    absent: bool,
}

impl File {
    /// Uploads the object in one request.
    pub fn write(&self) -> Write {
        let storage = &self.storage;

        Write {
            request: storage.client().put_object().bucket(storage.bucket()).key(&self.key),
            absent: false,
        }
    }
}

impl Write {
    /// The object's contents. Without a body, the object is empty.
    pub fn body(mut self, body: impl Into<ByteStream>) -> Self {
        self.request = self.request.body(body.into());
        self
    }

    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.request = self.request.content_type(content_type);
        self
    }

    /// The exact body size in bytes.
    ///
    /// - When presigning, the size becomes a signed header: the uploader must send exactly this many bytes, and the
    ///   bucket's CORS `AllowedHeaders` must allow `Content-Length`. The same holds for [`content_type`](Write::content_type).
    pub fn size(mut self, size: u64) -> Self {
        self.request = self.request.content_length(i64::try_from(size).unwrap_or(i64::MAX));
        self
    }

    /// Writes only when no object exists at the key, failing with [`Error::AlreadyExists`] otherwise.
    ///
    /// - Of concurrent writers, exactly one succeeds. A writer racing an in-flight upload may get
    ///   [`Error::Unavailable`] (HTTP 409) and should retry.
    pub fn if_absent(mut self) -> Self {
        self.request = self.request.if_none_match("*");
        self.absent = true;
        self
    }

    /// Encodes `value` as the JSON body and sets `Content-Type: application/json`.
    ///
    /// - A value serde cannot encode, such as a map with non-string keys, fails with [`Error::Invalid`].
    #[cfg(feature = "serde")]
    pub fn json<T: serde::Serialize + ?Sized>(self, value: &T) -> Result<Self> {
        let body = serde_json::to_vec(value).map_err(Error::invalid)?;

        Ok(self.body(body).content_type("application/json"))
    }

    /// Adjusts the SDK request for options this builder does not wrap, such as `cache_control`.
    pub fn with(mut self, adjust: impl FnOnce(PutObjectFluentBuilder) -> PutObjectFluentBuilder) -> Self {
        self.request = adjust(self.request);
        self
    }

    /// Signs the request instead of sending it, so another party can upload with the returned method, URL, and
    /// headers. The body is not part of the signature.
    pub async fn presign(self, expires_in: Duration) -> Result<PresignedRequest> {
        Ok(self.request.presigned(presigning(expires_in)?).await?)
    }
}

impl IntoFuture for Write {
    type Output = Result<Written>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let output = match self.request.send().await.map_err(Error::from) {
                Ok(output) => output,
                Err(error) if self.absent => return Err(error.absent()),
                Err(error) => return Err(error),
            };

            Ok(Written {
                etag: output.e_tag,
                version: output.version_id,
            })
        })
    }
}

/// The object a write or copy produced
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Written {
    /// Entity tag, quoted as S3 returns it
    pub etag: Option<String>,
    /// Version ID in a versioned bucket
    pub version: Option<String>,
}
