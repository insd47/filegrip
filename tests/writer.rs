mod common;

use common::{Sent, error, headers, status, storage};
use filegrip::Error;
use std::sync::{Arc, Mutex};

const MIB: usize = 1024 * 1024;
const CREATED: &str = "<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>";
const COMPLETED: &str = "<CompleteMultipartUploadResult><ETag>\"whole\"</ETag></CompleteMultipartUploadResult>";

fn part(number: usize) -> http::Response<String> {
    headers(&[("etag", &format!("\"part-{number}\""))])
}

/// The body size of an `UploadPart` request, whether or not the SDK framed it for a trailing checksum.
fn size(sent: &Sent) -> usize {
    let length = sent
        .header("x-amz-decoded-content-length")
        .or(sent.header("content-length"));

    length.unwrap().parse().unwrap()
}

#[tokio::test]
async fn cuts_uneven_writes_into_equal_parts_and_completes_in_order() {
    let responses = vec![status(200, CREATED), part(1), part(2), part(3), status(200, COMPLETED)];
    let (storage, replay) = storage(responses);

    let mut writer = storage.file("big").writer().part_size(5 * MIB).await.unwrap();
    writer.write(vec![0; 3 * MIB]).await.unwrap();
    writer.write(vec![0; 6 * MIB]).await.unwrap();
    writer.write(vec![0; 3 * MIB]).await.unwrap();
    let written = writer.close().await.unwrap();

    assert_eq!(written.etag.as_deref(), Some("\"whole\""));
    let requests = replay.requests();
    let sizes = requests[1..4].iter().map(size).collect::<Vec<_>>();
    assert_eq!(sizes, [5 * MIB, 5 * MIB, 2 * MIB]);
    assert!(requests[1].uri.contains("partNumber=1") && requests[1].uri.contains("uploadId=upload"));

    let complete = requests[4].text();
    let first = complete.find("<PartNumber>1</PartNumber>").unwrap();
    let third = complete.find("<PartNumber>3</PartNumber>").unwrap();
    assert!(first < third && complete.contains("part-2"), "{complete}");
}

#[tokio::test]
async fn completes_an_empty_upload_with_one_empty_part() {
    let (storage, replay) = storage(vec![status(200, CREATED), part(1), status(200, COMPLETED)]);

    storage.file("empty").writer().await.unwrap().close().await.unwrap();

    assert_eq!(size(&replay.requests()[1]), 0);
}

#[tokio::test]
async fn starts_the_upload_with_metadata() {
    let (storage, replay) = storage(vec![status(200, CREATED), part(1), status(200, COMPLETED)]);

    let writer = storage.file("a").writer().metadata("sha256", "e3b0c442").await.unwrap();
    writer.close().await.unwrap();

    assert_eq!(replay.requests()[0].header("x-amz-meta-sha256"), Some("e3b0c442"));
}

#[tokio::test]
async fn keeps_several_parts_in_flight() {
    let responses = vec![status(200, CREATED), part(1), part(2), part(3), status(200, COMPLETED)];
    let (storage, replay) = storage(responses);

    let mut writer = storage
        .file("big")
        .writer()
        .part_size(5 * MIB)
        .concurrent(4)
        .await
        .unwrap();
    writer.write(vec![0; 15 * MIB]).await.unwrap();
    writer.close().await.unwrap();

    let complete = replay.requests()[4].text();
    assert_eq!(complete.matches("<PartNumber>").count(), 3, "{complete}");
}

#[tokio::test]
async fn reports_accepted_bytes_per_part_through_close() {
    let responses = vec![status(200, CREATED), part(1), part(2), part(3), status(200, COMPLETED)];
    let (storage, _) = storage(responses);
    let totals = Arc::new(Mutex::new(Vec::new()));
    let seen = totals.clone();

    let mut writer = storage
        .file("big")
        .writer()
        .part_size(5 * MIB)
        .concurrent(4)
        .progress(move |uploaded| seen.lock().unwrap().push(uploaded))
        .await
        .unwrap();
    writer.write(vec![0; 12 * MIB]).await.unwrap();
    assert!(totals.lock().unwrap().is_empty(), "parts still in flight count nothing");
    writer.close().await.unwrap();

    let totals = totals.lock().unwrap();
    assert_eq!(*totals, [5 * MIB as u64, 10 * MIB as u64, 12 * MIB as u64]);
}

#[tokio::test]
async fn aborts_when_an_absent_upload_finds_an_object() {
    let responses = vec![
        status(200, CREATED),
        part(1),
        error(412, "PreconditionFailed"),
        status(204, ""),
    ];
    let (storage, replay) = storage(responses);

    let mut writer = storage.file("a").writer().if_absent().await.unwrap();
    writer.write(b"hello".to_vec()).await.unwrap();
    let error = writer.close().await.unwrap_err();

    assert!(matches!(error, Error::AlreadyExists { .. }));
    let requests = replay.requests();
    assert_eq!(requests[2].header("if-none-match"), Some("*"));
    assert_eq!(requests[3].method, "DELETE");
    assert!(requests[3].uri.contains("uploadId=upload"));
}

#[tokio::test]
async fn abort_deletes_the_upload() {
    let (storage, replay) = storage(vec![status(200, CREATED), status(204, "")]);

    storage.file("a").writer().await.unwrap().abort().await.unwrap();

    assert_eq!(replay.requests()[1].method, "DELETE");
}

#[tokio::test]
async fn rejects_parts_under_five_mib_before_starting() {
    let (storage, replay) = storage(vec![]);

    let error = storage.file("a").writer().part_size(5 * MIB - 1).await.unwrap_err();

    assert!(matches!(error, Error::Invalid { .. }));
    assert!(replay.requests().is_empty());
}
