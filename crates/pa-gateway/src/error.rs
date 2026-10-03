/// Gateway failures with stable, content-free public messages.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("authentication required")]
    Unauthenticated,
    #[error("session not found")]
    NotFound,
    #[error("operation forbidden")]
    Forbidden,
    #[error("session changed; reload before retrying")]
    Conflict,
    #[error("session is not ready")]
    NotReady,
    /// A configured quota or concurrency limit is exhausted.
    #[error("request exceeds the configured limit")]
    LimitExceeded,
    /// The request payload is larger than accepted.
    #[error("request is too large")]
    TooLarge,
    #[error("invalid request")]
    InvalidRequest,
    #[error("storage unavailable")]
    Storage(#[source] anyhow::Error),
    /// Delivery may have succeeded. Never automatically retry a mutation.
    #[error("runtime unavailable; delivery outcome may be unknown")]
    Runtime(#[source] anyhow::Error),
}

impl Error {
    /// Stable machine-readable identifier, distinct for every variant.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::NotFound => "not_found",
            Self::Forbidden => "forbidden",
            Self::Conflict => "conflict",
            Self::NotReady => "not_ready",
            Self::LimitExceeded => "limit_exceeded",
            Self::TooLarge => "too_large",
            Self::InvalidRequest => "invalid_request",
            Self::Storage(_) => "storage_unavailable",
            Self::Runtime(_) => "runtime_unavailable",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
