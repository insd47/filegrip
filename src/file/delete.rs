use super::{File, presigning};
use crate::{Error, Result};
use aws_sdk_s3::operation::delete_object::builders::DeleteObjectFluentBuilder;
use aws_sdk_s3::presigning::PresignedRequest;
use futures_util::future::BoxFuture;
use std::future::IntoFuture;
use std::time::Duration;

/// A deletion of one object, built by [`File::delete`].
///
/// - Succeeds when the object is already missing, including on S3-compatible services that answer 404.
/// - In a versioned bucket, adds a delete marker instead of removing data.
#[must_use = "a request does nothing until it is awaited"]
pub struct Delete {
    request: DeleteObjectFluentBuilder,
}

impl File {
    /// Deletes the object.
    pub fn delete(&self) -> Delete {
        let storage = &self.storage;

        Delete {
            request: storage.client().delete_object().bucket(storage.bucket()).key(&self.key),
        }
    }
}

impl Delete {
    /// Adjusts the SDK request for options this builder does not wrap.
    pub fn with(mut self, adjust: impl FnOnce(DeleteObjectFluentBuilder) -> DeleteObjectFluentBuilder) -> Self {
        self.request = adjust(self.request);
        self
    }

    /// Signs the request instead of sending it.
    pub async fn presign(self, expires_in: Duration) -> Result<PresignedRequest> {
        Ok(self.request.presigned(presigning(expires_in)?).await?)
    }
}

impl IntoFuture for Delete {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            match self.request.send().await.map_err(Error::from) {
                Err(error) if !error.is_not_found() => Err(error),
                _ => Ok(()),
            }
        })
    }
}
