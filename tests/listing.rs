mod common;

use common::{status, storage};
use filegrip::Child;
use futures_util::TryStreamExt;

#[tokio::test]
async fn list_follows_every_page() {
    let first = "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken>\
        <Contents><Key>videos/a.mp4</Key><Size>1</Size></Contents></ListBucketResult>";
    let second = "<ListBucketResult><IsTruncated>false</IsTruncated>\
        <Contents><Key>videos/2026/b.mp4</Key><Size>2</Size></Contents></ListBucketResult>";
    let (storage, replay) = storage(vec![status(200, first), status(200, second)]);

    let entries = storage.list("videos/").try_collect::<Vec<_>>().await.unwrap();

    let keys = entries
        .iter()
        .map(|entry| (entry.file.key(), entry.size))
        .collect::<Vec<_>>();
    assert_eq!(keys, [("videos/a.mp4", 1), ("videos/2026/b.mp4", 2)]);
    assert!(replay.requests()[1].uri.contains("continuation-token=next"));
}

#[tokio::test]
async fn children_split_prefixes_from_files_at_slashes() {
    let page = "<ListBucketResult><IsTruncated>false</IsTruncated>\
        <Contents><Key>videos/a.mp4</Key><Size>1</Size></Contents>\
        <CommonPrefixes><Prefix>videos/2026/</Prefix></CommonPrefixes></ListBucketResult>";
    let (storage, replay) = storage(vec![status(200, page)]);

    let children = storage.children("videos/").try_collect::<Vec<_>>().await.unwrap();

    assert!(matches!(&children[0], Child::Prefix(prefix) if prefix == "videos/2026/"));
    assert!(matches!(&children[1], Child::File(entry) if entry.file.key() == "videos/a.mp4"));
    assert!(replay.requests()[0].uri.contains("delimiter=%2F"));
}
