use crate::{Error, Result, Storage};
use aws_sdk_s3::presigning::PresigningConfig;
use std::time::Duration;

mod copy;
mod delete;
mod head;
mod read;
mod write;
mod writer;

pub use copy::CopyTo;
pub use delete::Delete;
pub use head::{Head, Stat};
pub use read::Read;
pub use write::{Write, Written};
pub use writer::{Upload, Writer};

/// One object key in a [`Storage`].
///
/// - Owned, `Clone`, and `'static`: keep it in a struct or move it into a task.
/// - The key is used exactly as given, without normalization.
/// - Every operation is a builder that runs when awaited. Options the builder does not wrap go to the SDK builder
///   inside through `with(..)`.
#[derive(Debug, Clone)]
pub struct File {
    storage: Storage,
    key: String,
}

impl File {
    /// The object key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Takes the object key out of the handle.
    pub fn into_key(self) -> String {
        self.key
    }
}

impl Storage {
    /// A handle to the object at `key`. Nothing is sent until an operation on it is awaited.
    pub fn file(&self, key: impl Into<String>) -> File {
        File {
            storage: self.clone(),
            key: key.into(),
        }
    }
}

/// Presigning configuration for `expires_in`, at most one week.
///
/// - A URL stops working when the credentials that signed it expire, which can be much sooner than `expires_in`.
fn presigning(expires_in: Duration) -> Result<PresigningConfig> {
    PresigningConfig::expires_in(expires_in).map_err(Error::invalid)
}
