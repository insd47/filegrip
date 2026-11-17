# FileGrip

File handles over `aws-sdk-s3` for Rust.

`filegrip` turns a bucket into a `Storage` and an object key into a `File` you can stat, read, write, delete, copy,
and presign. It speaks S3 and nothing more: key layouts and access policy belong to your code, and every option it does
not wrap stays one call away on the SDK's own builder.

```rust
use aws_sdk_s3::Client;
use filegrip::{Result, Storage};
use std::time::Duration;

const FIVE_MINUTES: Duration = Duration::from_secs(300);

async fn publish(client: &Client, jpeg: Vec<u8>) -> Result<String> {
    // A storage is a client plus a bucket name; a file is a storage plus a key.
    let storage = Storage::new(client, "media");
    let file = storage.file("photos/a.jpg");

    // Every operation is a builder that runs when awaited.
    file.write().body(jpeg).content_type("image/jpeg").await?;

    // The same builders presign instead of sending.
    file.read().url(FIVE_MINUTES).await
}
```

## Installation

```sh
cargo add filegrip aws-sdk-s3
```

Build the `aws_sdk_s3::Client` yourself, usually through `aws-config`, and hand it to each `Storage`. Enable the
`serde` feature for `read().json()` and `write().json(..)`.

## Operations

| Call                                                  | Resolves to                                         |
|-------------------------------------------------------|-----------------------------------------------------|
| `file.stat()`                                         | `Option<Stat>`                                      |
| `file.read().range(0..1024)`                          | `Option<ByteStream>`                                |
| `file.write().body(bytes).content_type("video/mp4")`  | `Written { etag, version }`                         |
| `file.write().body(bytes).if_absent()`                | `Written`, or `Error::AlreadyExists`                |
| `file.writer().part_size(16 << 20)`                   | `Writer`, a multipart upload: `write`, `close`, `abort` |
| `file.delete()`                                       | `()`, also when the object is missing               |
| `file.copy(&storage.file("b"))`                       | `Written`                                           |
| `storage.list("videos/")`                             | `Stream<Item = Result<Entry>>`, recursive           |
| `storage.children("videos/")`                         | `Stream<Item = Result<Child>>`, one level           |
| `storage.delete(keys)`                                | `()`, 1000 keys per request                         |

- `stat`, `read`, `write`, and `delete` also end in `.presign(expires_in)`, which returns the SDK's
  `PresignedRequest`. A presigned write carries its signed headers; the uploader must send them as returned.
- Options a builder does not wrap go to the SDK builder inside: `.with(|put| put.cache_control("max-age=60"))`.
  `storage.client()` reaches operations the crate does not wrap at all.
- Errors are split by what the caller does next: `NotFound`, `AlreadyExists`, `Precondition`, `Denied`, `Invalid`,
  and `Unavailable`. Each keeps the SDK error as its source and shows the S3 error code and request ID.

Deleting everything under a prefix composes a listing with a batch deletion:

```rust
use futures_util::TryStreamExt;

storage.delete(storage.list("problems/sort/").map_ok(|entry| entry.file.into_key())).await?;
```

## Things S3 does that you should know

- Without `s3:ListBucket`, a missing object answers 403 (`Error::Denied`) instead of 404.
- A HEAD response has no body, so `stat` reads every 404 as `None`, including a missing bucket. `read` tells
  `NoSuchKey` (`None`) from `NoSuchBucket` (`Error::Invalid`).
- A range that starts past the end of the object is `Error::Invalid` (416), not `None`.
- One `write` or `copy` carries at most 5 GB; use `writer` for more. There is no `rename`: it would be a copy and a
  delete, and not atomic. Copies go through the target's client, so copies across regions are out of scope.
- A body that is not read to the end holds its connection until it is dropped.
- A `Writer` that is dropped without `close` or `abort` leaves its parts in the bucket, billed but invisible. Add a
  lifecycle rule with `AbortIncompleteMultipartUpload` for uploads cut short by crashes.
- S3 has no transactions. To publish several objects together, write the others first and a marker object last
  with `if_absent()`; readers treat the marker as the commit.
- Presigned size and content type are signed headers: the browser must send the same headers, and the bucket's CORS
  `AllowedHeaders` must list them.
- A presigned URL lasts at most one week, and no longer than the credentials that signed it.

## S3-compatible services

The crate uses the client as configured and changes nothing on it. Since early 2025 the SDK sends checksums by default
([awslabs/aws-sdk-rust#1240](https://github.com/awslabs/aws-sdk-rust/issues/1240)), which some services reject. Set
`request_checksum_calculation(RequestChecksumCalculation::WhenRequired)` for those, `force_path_style(true)` for
MinIO, and the `auto` region for Cloudflare R2.

## Testing

There is no trait to mock. Test against the SDK: `aws-smithy-http-client`'s `test-util` replays canned responses
through a real `Client`, `aws-smithy-mocks` answers per operation, and MinIO runs the real protocol. If your code needs
a seam, put the trait on your own storage layer, where the domain operations are.
