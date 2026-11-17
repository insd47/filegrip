//! File handles over `aws-sdk-s3`.
//!
//! `filegrip` turns a bucket into a [`Storage`] and an object key into a [`File`].
//! It speaks S3 and nothing more: key layouts and access policy belong to the
//! calling code, and every option it does not wrap stays one call away on the
//! SDK's own builder through `with(..)`.
//!
//! ```no_run
//! use aws_sdk_s3::Client;
//! use filegrip::{Result, Storage};
//! use std::time::Duration;
//!
//! const FIVE_MINUTES: Duration = Duration::from_secs(300);
//!
//! async fn publish(client: &Client, jpeg: Vec<u8>) -> Result<String> {
//!     let storage = Storage::new(client, "media");
//!     let file = storage.file("photos/a.jpg");
//!
//!     file.write().body(jpeg).content_type("image/jpeg").await?;
//!     file.read().url(FIVE_MINUTES).await
//! }
//! ```
//!
//! # Operations
//!
//! Each operation on a [`File`] is a builder that runs when awaited:
//! [`stat`](File::stat), [`read`](File::read), [`write`](File::write),
//! [`writer`](File::writer), [`delete`](File::delete), and [`copy`](File::copy).
//! The builders of `stat`, `read`, `write`, and `delete` can also
//! `presign(expires_in)` instead of sending. A [`Storage`] lists its objects
//! with [`list`](Storage::list) and [`children`](Storage::children) and deletes
//! many at once with [`delete`](Storage::delete).
//!
//! # Absence and errors
//!
//! A missing object is a value, not an error: `stat` and `read` resolve to
//! `None`, and deleting a missing object succeeds. Everything else is an
//! [`Error`] split by what the caller does next. Without `s3:ListBucket`, S3
//! answers 403 instead of 404, so a missing object becomes [`Error::Denied`].

mod error;
mod file;
mod storage;

pub use error::{BoxError, Error, Result};
pub use file::{CopyTo, Delete, File, Head, Read, Stat, Upload, Write, Writer, Written};
pub use storage::{BatchDelete, Child, Entry, Storage};
