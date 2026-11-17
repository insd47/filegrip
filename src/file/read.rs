use super::{File, presigning};
use crate::{Error, Result};
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::presigning::PresignedRequest;
use aws_sdk_s3::primitives::ByteStream;
use futures_util::future::BoxFuture;
use std::future::IntoFuture;
use std::ops::{Bound, RangeBounds};
use std::time::Duration;

/// A download of one object, built by [`File::read`].
///
/// - Resolves to `None` when the object does not exist (`NoSuchKey`). A missing bucket is an error.
/// - A body that is not read to the end keeps its connection busy until it is dropped.
#[must_use = "a request does nothing until it is awaited"]
pub struct Read {
    request: GetObjectFluentBuilder,
    empty: bool,
}

impl File {
    /// Downloads the object.
    pub fn read(&self) -> Read {
        let storage = &self.storage;

        Read {
            request: storage.client().get_object().bucket(storage.bucket()).key(&self.key),
            empty: false,
        }
    }
}

impl Read {
    /// Downloads only the bytes in `range`, such as `0..1024` or `512..`.
    ///
    /// - A range that starts past the end of the object fails with [`Error::Invalid`] (HTTP 416), not `None`.
    /// - An empty range fails with [`Error::Invalid`], since HTTP cannot ask for zero bytes.
    pub fn range(mut self, range: impl RangeBounds<u64>) -> Self {
        let start = match range.start_bound() {
            Bound::Included(&start) => Some(start),
            Bound::Excluded(&start) => start.checked_add(1),
            Bound::Unbounded => Some(0),
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => Some(Some(end)),
            Bound::Excluded(&end) => end.checked_sub(1).map(Some),
            Bound::Unbounded => Some(None),
        };

        match (start, end) {
            (Some(start), Some(None)) => self.request = self.request.range(format!("bytes={start}-")),
            (Some(start), Some(Some(end))) if start <= end => {
                self.request = self.request.range(format!("bytes={start}-{end}"))
            }
            _ => self.empty = true,
        }

        self
    }

    /// Adjusts the SDK request for options this builder does not wrap.
    pub fn with(mut self, adjust: impl FnOnce(GetObjectFluentBuilder) -> GetObjectFluentBuilder) -> Self {
        self.request = adjust(self.request);
        self
    }

    /// Signs the request instead of sending it.
    pub async fn presign(self, expires_in: Duration) -> Result<PresignedRequest> {
        Ok(self.request.presigned(presigning(expires_in)?).await?)
    }

    /// Signs the request and returns only its URL, which a browser can open as is.
    pub async fn url(self, expires_in: Duration) -> Result<String> {
        Ok(self.presign(expires_in).await?.uri().to_owned())
    }

    /// Downloads the object and decodes it as JSON.
    ///
    /// - A body that does not fit `T` fails with [`Error::Invalid`].
    #[cfg(feature = "serde")]
    pub async fn json<T: serde::de::DeserializeOwned>(self) -> Result<Option<T>> {
        let Some(body) = self.await? else {
            return Ok(None);
        };
        let bytes = body.collect().await.map_err(Error::unavailable)?.into_bytes();

        serde_json::from_slice(&bytes).map(Some).map_err(Error::invalid)
    }
}

impl IntoFuture for Read {
    type Output = Result<Option<ByteStream>>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            if self.empty {
                return Err(Error::invalid("the range is empty"));
            }

            match self.request.send().await.map_err(Error::from) {
                Ok(output) => Ok(Some(output.body)),
                Err(error) if error.is_not_found() => Ok(None),
                Err(error) => Err(error),
            }
        })
    }
}
