mod common;

use common::{error, headers, status, storage};
use filegrip::{Error, File, Storage};
use std::error::Error as _;

#[tokio::test]
async fn stat_reads_the_head_response() {
    let (storage, _) = storage(vec![headers(&[
        ("content-length", "42"),
        ("content-type", "video/mp4"),
        ("etag", "\"abc\""),
    ])]);

    let stat = storage.file("videos/a.mp4").stat().await.unwrap().unwrap();

    assert_eq!(stat.size, 42);
    assert_eq!(stat.content_type.as_deref(), Some("video/mp4"));
    assert_eq!(stat.etag.as_deref(), Some("\"abc\""));
}

#[tokio::test]
async fn stat_returns_none_for_a_bodiless_404() {
    let (storage, _) = storage(vec![status(404, "")]);

    assert!(storage.file("missing").stat().await.unwrap().is_none());
}

#[tokio::test]
async fn read_returns_none_for_a_missing_key_but_fails_for_a_missing_bucket() {
    let (storage, _) = storage(vec![error(404, "NoSuchKey"), error(404, "NoSuchBucket")]);

    assert!(storage.file("missing").read().await.unwrap().is_none());
    assert!(matches!(
        storage.file("missing").read().await.unwrap_err(),
        Error::Invalid { .. }
    ));
}

#[tokio::test]
async fn read_sends_inclusive_byte_ranges() {
    let (storage, replay) = storage(vec![status(206, "a"), status(206, "b"), status(206, "c")]);
    let file = storage.file("a");

    file.read().range(0..1024).await.unwrap();
    file.read().range(512..).await.unwrap();
    file.read().range(..=9).await.unwrap();

    let ranges = replay
        .requests()
        .iter()
        .map(|sent| sent.header("range").unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(ranges, ["bytes=0-1023", "bytes=512-", "bytes=0-9"]);
}

#[tokio::test]
async fn read_rejects_an_empty_range_without_sending() {
    let (storage, replay) = storage(vec![]);

    assert!(matches!(
        storage.file("a").read().range(5..5).await.unwrap_err(),
        Error::Invalid { .. }
    ));
    assert!(matches!(
        storage.file("a").read().range(..0).await.unwrap_err(),
        Error::Invalid { .. }
    ));
    assert!(replay.requests().is_empty());
}

#[tokio::test]
async fn read_past_the_end_is_invalid_not_none() {
    let (storage, _) = storage(vec![error(416, "InvalidRange")]);

    assert!(matches!(
        storage.file("a").read().range(10..).await.unwrap_err(),
        Error::Invalid { .. }
    ));
}

#[tokio::test]
async fn write_returns_the_etag_and_version() {
    let (storage, replay) = storage(vec![headers(&[("etag", "\"abc\""), ("x-amz-version-id", "v1")])]);

    let written = storage
        .file("a")
        .write()
        .body(b"hello".to_vec())
        .content_type("text/plain")
        .await
        .unwrap();

    assert_eq!(written.etag.as_deref(), Some("\"abc\""));
    assert_eq!(written.version.as_deref(), Some("v1"));
    assert_eq!(replay.requests()[0].header("content-type"), Some("text/plain"));
}

#[tokio::test]
async fn if_absent_turns_a_412_into_already_exists() {
    let (storage, replay) = storage(vec![error(412, "PreconditionFailed")]);

    let error = storage
        .file("a")
        .write()
        .body(b"x".to_vec())
        .if_absent()
        .await
        .unwrap_err();

    assert!(matches!(error, Error::AlreadyExists { .. }));
    assert_eq!(replay.requests()[0].header("if-none-match"), Some("*"));
}

#[tokio::test]
async fn a_conflicting_conditional_write_is_retryable() {
    let (storage, _) = storage(vec![error(409, "ConditionalRequestConflict")]);

    let error = storage.file("a").write().if_absent().await.unwrap_err();

    assert!(matches!(error, Error::Unavailable { .. }));
}

#[tokio::test]
async fn delete_succeeds_for_a_missing_object() {
    let (storage, _) = storage(vec![status(204, ""), error(404, "NoSuchKey")]);

    storage.file("a").delete().await.unwrap();
    storage.file("a").delete().await.unwrap();
}

#[tokio::test]
async fn copy_encodes_the_source_key_once_and_keeps_slashes() {
    let body = "<CopyObjectResult><ETag>\"abc\"</ETag></CopyObjectResult>";
    let (storage, replay) = storage(vec![status(200, body)]);
    let target = Storage::new(storage.client(), "other").file("b");

    let written = storage.file("dir/a b+c/é.txt").copy(&target).await.unwrap();

    assert_eq!(written.etag.as_deref(), Some("\"abc\""));
    let sent = &replay.requests()[0];
    assert_eq!(
        sent.header("x-amz-copy-source"),
        Some("bucket/dir/a%20b%2Bc/%C3%A9.txt")
    );
    assert!(sent.uri.contains("/other/b"), "{}", sent.uri);
}

#[tokio::test]
async fn copy_from_a_missing_source_is_not_found() {
    let (storage, _) = storage(vec![error(404, "NoSuchKey")]);

    let error = storage.file("a").copy(&storage.file("b")).await.unwrap_err();

    assert!(matches!(error, Error::NotFound { .. }));
}

#[tokio::test]
async fn errors_are_classified_by_code_before_status() {
    let cases = [
        (error(403, "AccessDenied"), "Denied"),
        (error(400, "RequestTimeout"), "Unavailable"),
        (error(503, "SlowDown"), "Unavailable"),
        (error(400, "InvalidArgument"), "Invalid"),
        (error(500, "Unknown"), "Unavailable"),
    ];

    for (response, kind) in cases {
        let (storage, _) = storage(vec![response]);
        let error = storage.file("a").write().await.unwrap_err();

        let actual = match error {
            Error::Denied { .. } => "Denied",
            Error::Invalid { .. } => "Invalid",
            Error::Unavailable { .. } => "Unavailable",
            _ => "other",
        };
        assert_eq!(actual, kind, "{error}");
    }
}

#[tokio::test]
async fn errors_show_the_code_and_request_id_and_keep_the_source() {
    let (storage, _) = storage(vec![error(403, "AccessDenied")]);

    let error = storage.file("a").write().await.unwrap_err();

    assert_eq!(error.to_string(), "access denied (AccessDenied, request REQUEST)");
    assert_eq!(error.code(), Some("AccessDenied"));
    assert!(error.source().is_some());
}

#[test]
fn handles_and_operations_cross_tasks() {
    fn task<T: Send + Sync + 'static>() {}
    fn future<T: std::future::IntoFuture<IntoFuture: Send + 'static>>() {}

    task::<Storage>();
    task::<File>();
    future::<filegrip::Head>();
    future::<filegrip::Read>();
    future::<filegrip::Write>();
    future::<filegrip::Delete>();
    future::<filegrip::CopyTo>();
    future::<filegrip::Upload>();
}
