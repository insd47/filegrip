use super::Storage;
use crate::{Error, Result};
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::operation::delete_objects::builders::DeleteObjectsFluentBuilder;
use aws_sdk_s3::types::{self, ObjectIdentifier};
use futures_util::future::BoxFuture;
use futures_util::stream::{Stream, TryStreamExt};
use std::fmt;
use std::future::IntoFuture;

/// Keys per `DeleteObjects` request, the most S3 accepts
const BATCH_SIZE: usize = 1000;

type Adjust = Box<dyn Fn(DeleteObjectsFluentBuilder) -> DeleteObjectsFluentBuilder + Send + Sync>;

/// A deletion of many objects, built by [`Storage::delete`].
///
/// - Sends one `DeleteObjects` request per 1000 keys, reading the key stream as it goes.
/// - Missing objects count as deleted. Deletion is idempotent, so after a failure, run the whole deletion again.
/// - Not atomic: batches sent before a failure stay deleted, and objects created while a listing is being deleted
///   remain.
#[must_use = "a request does nothing until it is awaited"]
pub struct BatchDelete<S> {
    storage: Storage,
    keys: S,
    adjust: Option<Adjust>,
}

impl Storage {
    /// Deletes every key in `keys`, such as the keys of a [`list`](Storage::list):
    ///
    /// ```no_run
    /// # async fn purge(storage: &filegrip::Storage) -> filegrip::Result<()> {
    /// use futures_util::TryStreamExt;
    ///
    /// storage.delete(storage.list("problems/sort/").map_ok(|entry| entry.file.into_key())).await
    /// # }
    /// ```
    ///
    /// - Takes keys rather than [`File`](crate::File)s, so every object is in this bucket.
    /// - Stops at the first error in `keys` and returns it.
    pub fn delete<S, K>(&self, keys: S) -> BatchDelete<S>
    where
        S: Stream<Item = Result<K>> + Send + 'static,
        K: Into<String> + Send + 'static,
    {
        BatchDelete {
            storage: self.clone(),
            keys,
            adjust: None,
        }
    }
}

impl<S> BatchDelete<S> {
    /// Adjusts every SDK request for options this builder does not wrap, such as `bypass_governance_retention`.
    pub fn with(
        mut self,
        adjust: impl Fn(DeleteObjectsFluentBuilder) -> DeleteObjectsFluentBuilder + Send + Sync + 'static,
    ) -> Self {
        self.adjust = Some(Box::new(adjust));
        self
    }
}

impl<S, K> IntoFuture for BatchDelete<S>
where
    S: Stream<Item = Result<K>> + Send + 'static,
    K: Into<String> + Send + 'static,
{
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let Self { storage, keys, adjust } = self;
            let mut batches = Box::pin(keys.try_chunks(BATCH_SIZE));

            while let Some(batch) = batches.try_next().await.map_err(|error| error.1)? {
                let objects = batch
                    .into_iter()
                    .map(|key| ObjectIdentifier::builder().key(key).build());
                let objects = objects.collect::<Result<Vec<_>, _>>().map_err(Error::invalid)?;
                let delete = types::Delete::builder().set_objects(Some(objects)).quiet(true).build();

                let mut request = storage.client().delete_objects().bucket(storage.bucket());
                request = request.delete(delete.map_err(Error::invalid)?);

                if let Some(adjust) = &adjust {
                    request = adjust(request);
                }

                let output = request.send().await?;

                // S3 answers 200 even when some keys fail, listing them in `Errors`.
                let Some(first) = output.errors().first() else {
                    continue;
                };
                let code = first.code().map(str::to_owned);
                let request_id = output.request_id().map(str::to_owned);

                return Err(Error::reported(
                    code,
                    request_id,
                    Undeleted(output.errors.unwrap_or_default()),
                ));
            }

            Ok(())
        })
    }
}

/// The keys a `DeleteObjects` response reported as not deleted
#[derive(Debug)]
struct Undeleted(Vec<types::Error>);

impl fmt::Display for Undeleted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} keys were not deleted:", self.0.len())?;

        for error in &self.0 {
            let key = error.key().unwrap_or_default();
            let code = error.code().unwrap_or("unknown");
            let message = error.message().unwrap_or_default();
            write!(formatter, " {key} ({code}: {message});")?;
        }

        Ok(())
    }
}

impl std::error::Error for Undeleted {}
