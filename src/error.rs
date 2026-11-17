use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::RequestId;

/// The original failure kept behind [`std::error::Error::source`].
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Failure modes of S3 operations, split by what the caller does next.
///
/// - Every variant keeps the original error as its [`source`](std::error::Error::source), and its display shows the
///   S3 error code and request ID when S3 sent them.
/// - Expected absence is not an error: [`File::stat`](crate::File::stat) and [`File::read`](crate::File::read)
///   resolve to `None`, and deleting a missing object succeeds.
/// - New variants may be added in minor releases, so a `match` needs a wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The object does not exist, such as the source of a copy.
    #[error("object not found{}", detail(.code, .request_id))]
    #[non_exhaustive]
    NotFound {
        code: Option<String>,
        request_id: Option<String>,
        #[source]
        source: BoxError,
    },
    /// A write guarded by `if_absent()` found an object already there.
    #[error("object already exists{}", detail(.code, .request_id))]
    #[non_exhaustive]
    AlreadyExists {
        code: Option<String>,
        request_id: Option<String>,
        #[source]
        source: BoxError,
    },
    /// A conditional request was rejected (HTTP 412).
    #[error("precondition failed{}", detail(.code, .request_id))]
    #[non_exhaustive]
    Precondition {
        code: Option<String>,
        request_id: Option<String>,
        #[source]
        source: BoxError,
    },
    /// The credentials may not perform the request (HTTP 403).
    ///
    /// Without `s3:ListBucket`, S3 answers 403 instead of 404 for a missing object.
    #[error("access denied{}", detail(.code, .request_id))]
    #[non_exhaustive]
    Denied {
        code: Option<String>,
        request_id: Option<String>,
        #[source]
        source: BoxError,
    },
    /// The request cannot succeed as written, such as a range past the end of the object (HTTP 416), a missing
    /// bucket, a presign duration over one week, or a value that does not fit its serde model. Retrying does not help.
    #[error("invalid request{}", detail(.code, .request_id))]
    #[non_exhaustive]
    Invalid {
        code: Option<String>,
        request_id: Option<String>,
        #[source]
        source: BoxError,
    },
    /// S3 could not be reached or failed after the SDK's retries: network errors, timeouts, 5xx, `SlowDown`, or a
    /// conflicting conditional write (HTTP 409). The operation may succeed on retry.
    #[error("storage unavailable{}", detail(.code, .request_id))]
    #[non_exhaustive]
    Unavailable {
        code: Option<String>,
        request_id: Option<String>,
        #[source]
        source: BoxError,
    },
}

impl Error {
    /// The S3 error code, such as `NoSuchKey` or `SlowDown`.
    pub fn code(&self) -> Option<&str> {
        let (code, _) = self.ids();

        code.as_deref()
    }

    /// The S3 request ID, needed when contacting AWS support.
    pub fn request_id(&self) -> Option<&str> {
        let (_, request_id) = self.ids();

        request_id.as_deref()
    }

    fn ids(&self) -> (&Option<String>, &Option<String>) {
        match self {
            Self::NotFound { code, request_id, .. }
            | Self::AlreadyExists { code, request_id, .. }
            | Self::Precondition { code, request_id, .. }
            | Self::Denied { code, request_id, .. }
            | Self::Invalid { code, request_id, .. }
            | Self::Unavailable { code, request_id, .. } => (code, request_id),
        }
    }

    pub(crate) fn invalid(source: impl Into<BoxError>) -> Self {
        Kind::Invalid.error(None, None, source.into())
    }

    pub(crate) fn unavailable(source: impl Into<BoxError>) -> Self {
        Kind::Unavailable.error(None, None, source.into())
    }

    /// A failure reported per key inside a successful response, as `DeleteObjects` does.
    pub(crate) fn reported(code: Option<String>, request_id: Option<String>, source: impl Into<BoxError>) -> Self {
        Kind::classify(None, code.as_deref()).error(code, request_id, source.into())
    }

    /// Whether the error is a 404, which `stat` and `read` turn into `None`.
    pub(crate) fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }

    /// Reclassifies a rejected `If-None-Match: *` as [`Error::AlreadyExists`].
    pub(crate) fn absent(self) -> Self {
        match self {
            Self::Precondition {
                code,
                request_id,
                source,
            } => Kind::AlreadyExists.error(code, request_id, source),
            error => error,
        }
    }
}

impl<E> From<SdkError<E, HttpResponse>> for Error
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    fn from(error: SdkError<E, HttpResponse>) -> Self {
        let kind = match &error {
            SdkError::ConstructionFailure(_) => Kind::Invalid,
            SdkError::ServiceError(service) => {
                Kind::classify(Some(service.raw().status().as_u16()), service.err().code())
            }
            _ => Kind::Unavailable,
        };
        let code = error.code().map(str::to_owned);
        let request_id = error.request_id().map(str::to_owned);

        kind.error(code, request_id, Box::new(error))
    }
}

/// Convenience alias for results produced by this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

enum Kind {
    NotFound,
    AlreadyExists,
    Precondition,
    Denied,
    Invalid,
    Unavailable,
}

impl Kind {
    /// Reads the error code before the status: `RequestTimeout` arrives as 400 and `NoSuchBucket` as 404.
    fn classify(status: Option<u16>, code: Option<&str>) -> Self {
        match code {
            Some("NoSuchKey") => return Self::NotFound,
            Some("PreconditionFailed") => return Self::Precondition,
            Some("AccessDenied") => return Self::Denied,
            Some("NoSuchBucket") => return Self::Invalid,
            Some(
                "ConditionalRequestConflict"
                | "InternalError"
                | "OperationAborted"
                | "RequestTimeout"
                | "ServiceUnavailable"
                | "SlowDown",
            ) => return Self::Unavailable,
            _ => {}
        }

        match status {
            Some(404) => Self::NotFound,
            Some(412) => Self::Precondition,
            Some(403) => Self::Denied,
            Some(500..) | None => Self::Unavailable,
            Some(_) => Self::Invalid,
        }
    }

    fn error(self, code: Option<String>, request_id: Option<String>, source: BoxError) -> Error {
        match self {
            Self::NotFound => Error::NotFound {
                code,
                request_id,
                source,
            },
            Self::AlreadyExists => Error::AlreadyExists {
                code,
                request_id,
                source,
            },
            Self::Precondition => Error::Precondition {
                code,
                request_id,
                source,
            },
            Self::Denied => Error::Denied {
                code,
                request_id,
                source,
            },
            Self::Invalid => Error::Invalid {
                code,
                request_id,
                source,
            },
            Self::Unavailable => Error::Unavailable {
                code,
                request_id,
                source,
            },
        }
    }
}

fn detail(code: &Option<String>, request_id: &Option<String>) -> String {
    match (code, request_id) {
        (Some(code), Some(request_id)) => format!(" ({code}, request {request_id})"),
        (Some(code), None) => format!(" ({code})"),
        (None, Some(request_id)) => format!(" (request {request_id})"),
        (None, None) => String::new(),
    }
}
