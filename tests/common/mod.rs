//! An offline S3: test clients answer requests with canned responses and hand
//! the requests back for inspection.

#![allow(dead_code)] // each test crate uses a different subset

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::{Client, Config};
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use filegrip::Storage;

/// A storage on bucket `bucket` whose requests receive `responses` in order.
pub fn storage(responses: Vec<http::Response<String>>) -> (Storage, Replay) {
    let events = responses
        .into_iter()
        .map(|response| ReplayEvent::new(http::Request::new(SdkBody::empty()), response.map(SdkBody::from)))
        .collect();
    let replay = StaticReplayClient::new(events);
    let config = Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("access", "secret", None, None, "test"))
        .retry_config(RetryConfig::disabled())
        .force_path_style(true)
        .http_client(replay.clone())
        .build();

    (Storage::new(&Client::from_conf(config), "bucket"), Replay(replay))
}

/// A response with `status` and an XML or empty `body`.
pub fn status(status: u16, body: &str) -> http::Response<String> {
    http::Response::builder()
        .status(status)
        .header("x-amz-request-id", "REQUEST")
        .body(body.into())
        .unwrap()
}

/// A successful response with `headers` and an empty body.
pub fn headers(headers: &[(&str, &str)]) -> http::Response<String> {
    let mut response = http::Response::builder().status(200);

    for (name, value) in headers {
        response = response.header(*name, *value);
    }

    response.body(String::new()).unwrap()
}

/// An S3 error response, such as `error(404, "NoSuchKey")`.
pub fn error(status: u16, code: &str) -> http::Response<String> {
    let body = format!("<Error><Code>{code}</Code><Message>{code}</Message><RequestId>REQUEST</RequestId></Error>");

    self::status(status, &body)
}

pub struct Replay(StaticReplayClient);

impl Replay {
    /// The requests the client sent, in order.
    pub fn requests(&self) -> Vec<Sent> {
        self.0
            .actual_requests()
            .map(|request| Sent {
                method: request.method().to_owned(),
                uri: request.uri().to_owned(),
                headers: request
                    .headers()
                    .iter()
                    .map(|(name, value)| (name.to_owned(), value.to_owned()))
                    .collect(),
                body: request.body().bytes().map(<[u8]>::to_vec),
            })
            .collect()
    }
}

/// One request as the client sent it
#[derive(Debug)]
pub struct Sent {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

impl Sent {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn text(&self) -> String {
        String::from_utf8(self.body.clone().expect("in-memory body")).unwrap()
    }
}
