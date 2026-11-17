mod common;

use common::storage;
use filegrip::Error;
use std::time::Duration;

const FIVE_MINUTES: Duration = Duration::from_secs(300);

#[tokio::test]
async fn signs_the_upload_size_into_put_urls() {
    let (storage, replay) = storage(vec![]);
    let file = storage.file("object");

    let put = file.write().size(1).presign(FIVE_MINUTES).await.unwrap();
    let get = file.read().url(FIVE_MINUTES).await.unwrap();

    assert_eq!(put.method(), "PUT");
    assert!(
        put.uri().contains("X-Amz-SignedHeaders=content-length%3Bhost"),
        "{}",
        put.uri()
    );
    assert!(get.contains("X-Amz-SignedHeaders=host"), "{get}");
    assert!(replay.requests().is_empty());
}

#[tokio::test]
async fn signs_the_content_type_and_returns_it_as_a_header_to_send() {
    let (storage, _) = storage(vec![]);

    let put = storage
        .file("a.jpg")
        .write()
        .size(10)
        .content_type("image/jpeg")
        .presign(FIVE_MINUTES)
        .await
        .unwrap();

    assert!(
        put.uri()
            .contains("X-Amz-SignedHeaders=content-length%3Bcontent-type%3Bhost"),
        "{}",
        put.uri()
    );
    assert!(
        put.headers()
            .any(|(name, value)| name == "content-type" && value == "image/jpeg")
    );
}

#[tokio::test]
async fn presigns_head_and_delete() {
    let (storage, _) = storage(vec![]);
    let file = storage.file("a");

    assert_eq!(file.stat().presign(FIVE_MINUTES).await.unwrap().method(), "HEAD");
    assert_eq!(file.delete().presign(FIVE_MINUTES).await.unwrap().method(), "DELETE");
}

#[tokio::test]
async fn rejects_a_duration_over_one_week() {
    let (storage, _) = storage(vec![]);
    let eight_days = Duration::from_secs(8 * 24 * 60 * 60);

    let error = storage.file("a").read().presign(eight_days).await.unwrap_err();

    assert!(matches!(error, Error::Invalid { .. }));
}
