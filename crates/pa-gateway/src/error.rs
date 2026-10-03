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
    #[error("request exceeds the configured limit")]
    LimitExceeded,
    #[error("invalid request")]
    InvalidRequest,
    #[error("storage unavailable")]
    Storage(#[source] anyhow::Error),
    /// Delivery may have succeeded. Never automatically retry a mutation.
    #[error("runtime unavailable; delivery outcome may be unknown")]
    Runtime(#[source] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
