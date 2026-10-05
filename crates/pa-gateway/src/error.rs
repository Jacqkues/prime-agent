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
    /// An earlier attempt with the same idempotency key is in flight or has an
    /// unknown outcome. Reconcile session history before using a new key.
    #[error("an earlier attempt with this idempotency key has an unknown outcome")]
    IdempotencyUnresolved,
    #[error("storage unavailable")]
    Storage(#[source] anyhow::Error),
    /// The runtime was unreachable or explicitly refused the operation, so it
    /// certainly did not take effect. Safe to retry.
    #[error("runtime did not accept the operation")]
    NotDelivered(#[source] anyhow::Error),
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
            Self::IdempotencyUnresolved => "idempotency_unresolved",
            Self::Storage(_) => "storage_unavailable",
            Self::NotDelivered(_) => "not_delivered",
            Self::Runtime(_) => "runtime_unavailable",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
