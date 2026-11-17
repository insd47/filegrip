use aws_sdk_s3::Client;

mod delete;
mod list;

pub use delete::BatchDelete;
pub use list::{Child, Entry};

/// One bucket reached through one SDK client.
///
/// - Cloning is cheap; the clones share the client's connection pool.
/// - The client is used as configured. For S3-compatible services, configure it there: `force_path_style(true)` for
///   MinIO, the `auto` region for R2, and `RequestChecksumCalculation::WhenRequired` for services that reject the
///   checksums the SDK has sent by default since early 2025.
#[derive(Debug, Clone)]
pub struct Storage {
    client: Client,
    bucket: String,
}

impl Storage {
    /// Wraps a configured client and a bucket name.
    pub fn new(client: &Client, bucket: impl Into<String>) -> Self {
        Self {
            client: client.clone(),
            bucket: bucket.into(),
        }
    }

    /// The SDK client, for operations this crate does not wrap.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The bucket name
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
}
