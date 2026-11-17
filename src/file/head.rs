use super::{File, presigning};
use crate::{Error, Result};
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::head_object::builders::HeadObjectFluentBuilder;
use aws_sdk_s3::presigning::PresignedRequest;
use futures_util::future::BoxFuture;
use std::future::IntoFuture;
use std::time::{Duration, SystemTime};

/// A metadata lookup of one object, built by [`File::stat`].
///
/// - Resolves to `None` for any 404. A HEAD response has no body, so a missing bucket also reads as `None`.
#[must_use = "a request does nothing until it is awaited"]
pub struct Head {
    request: HeadObjectFluentBuilder,
}

impl File {
    /// Looks up the object's metadata.
    pub fn stat(&self) -> Head {
        let storage = &self.storage;

        Head {
            request: storage.client().head_object().bucket(storage.bucket()).key(&self.key),
        }
    }
}

impl Head {
    /// Adjusts the SDK request for options this builder does not wrap.
    pub fn with(mut self, adjust: impl FnOnce(HeadObjectFluentBuilder) -> HeadObjectFluentBuilder) -> Self {
        self.request = adjust(self.request);
        self
    }

    /// Signs the request instead of sending it.
    pub async fn presign(self, expires_in: Duration) -> Result<PresignedRequest> {
        Ok(self.request.presigned(presigning(expires_in)?).await?)
    }
}

impl IntoFuture for Head {
    type Output = Result<Option<Stat>>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            match self.request.send().await.map_err(Error::from) {
                Ok(output) => Ok(Some(Stat::from(output))),
                Err(error) if error.is_not_found() => Ok(None),
                Err(error) => Err(error),
            }
        })
    }
}

/// Metadata of one object
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Stat {
    /// Size in bytes
    pub size: u64,
    pub content_type: Option<String>,
    /// Entity tag, quoted as S3 returns it
    pub etag: Option<String>,
    pub modified: Option<SystemTime>,
    /// Version ID in a versioned bucket
    pub version: Option<String>,
}

impl From<HeadObjectOutput> for Stat {
    fn from(output: HeadObjectOutput) -> Self {
        Self {
            size: output
                .content_length
                .and_then(|size| u64::try_from(size).ok())
                .unwrap_or_default(),
            content_type: output.content_type,
            etag: output.e_tag,
            modified: output.last_modified.and_then(|time| SystemTime::try_from(time).ok()),
            version: output.version_id,
        }
    }
}
