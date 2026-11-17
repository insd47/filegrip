#![cfg(feature = "serde")]

mod common;

use common::{error, headers, status, storage};
use filegrip::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Config {
    name: String,
}

#[tokio::test]
async fn writes_json_with_its_content_type() {
    let (storage, replay) = storage(vec![headers(&[])]);
    let config = Config { name: "sort".into() };

    storage
        .file("sort/config.json")
        .write()
        .json(&config)
        .unwrap()
        .if_absent()
        .await
        .unwrap();

    let sent = &replay.requests()[0];
    assert_eq!(sent.header("content-type"), Some("application/json"));
    assert_eq!(sent.text(), r#"{"name":"sort"}"#);
}

#[tokio::test]
async fn reads_json_or_none() {
    let (storage, _) = storage(vec![status(200, r#"{"name":"sort"}"#), error(404, "NoSuchKey")]);
    let file = storage.file("sort/config.json");

    assert_eq!(
        file.read().json::<Config>().await.unwrap(),
        Some(Config { name: "sort".into() })
    );
    assert_eq!(file.read().json::<Config>().await.unwrap(), None);
}

#[tokio::test]
async fn a_body_that_does_not_fit_is_invalid() {
    let (storage, _) = storage(vec![status(200, "[]")]);

    let error = storage.file("a").read().json::<Config>().await.unwrap_err();

    assert!(matches!(error, Error::Invalid { .. }));
}
