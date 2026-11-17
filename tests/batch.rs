mod common;

use common::{error, status, storage};
use filegrip::Error;
use futures_util::{TryStreamExt, stream};
use std::error::Error as _;

const DELETED: &str = "<DeleteResult></DeleteResult>";

fn keys(count: usize) -> impl futures_util::Stream<Item = filegrip::Result<String>> + Send + 'static {
    stream::iter((0..count).map(|index| Ok(format!("key-{index}"))))
}

#[tokio::test]
async fn sends_nothing_for_no_keys() {
    let (storage, replay) = storage(vec![]);

    storage.delete(keys(0)).await.unwrap();

    assert!(replay.requests().is_empty());
}

#[tokio::test]
async fn sends_one_request_per_thousand_keys() {
    let (storage, replay) = storage(vec![status(200, DELETED), status(200, DELETED), status(200, DELETED)]);

    storage.delete(keys(1000)).await.unwrap();
    storage.delete(keys(1001)).await.unwrap();

    let counts = replay
        .requests()
        .iter()
        .map(|sent| sent.text().matches("<Key>").count())
        .collect::<Vec<_>>();
    assert_eq!(counts, [1000, 1000, 1]);
    assert!(replay.requests()[0].text().contains("<Quiet>true</Quiet>"));
}

#[tokio::test]
async fn fails_when_the_response_reports_undeleted_keys() {
    let body = "<DeleteResult><Error><Key>key-1</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>\
        </DeleteResult>";
    let (storage, _) = storage(vec![status(200, body)]);

    let error = storage.delete(keys(2)).await.unwrap_err();

    assert!(matches!(error, Error::Denied { .. }));
    assert!(error.source().unwrap().to_string().contains("key-1 (AccessDenied"));
}

#[tokio::test]
async fn deletes_what_a_listing_yields() {
    let page = "<ListBucketResult><IsTruncated>false</IsTruncated>\
        <Contents><Key>a</Key><Size>1</Size></Contents></ListBucketResult>";
    let (storage, replay) = storage(vec![status(200, page), status(200, DELETED)]);

    storage
        .delete(storage.list("").map_ok(|entry| entry.file.into_key()))
        .await
        .unwrap();

    assert!(replay.requests()[1].text().contains("<Key>a</Key>"));
}

#[tokio::test]
async fn returns_the_key_stream_error_without_deleting() {
    let (storage, replay) = storage(vec![error(503, "SlowDown")]);

    let error = storage
        .delete(storage.list("").map_ok(|entry| entry.file.into_key()))
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Unavailable { .. }));
    assert_eq!(replay.requests().len(), 1);
}
