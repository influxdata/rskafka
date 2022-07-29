use thiserror::Error;

pub use crate::messenger::RequestError;
pub use crate::protocol::error::Error as ProtocolError;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Connection error: {0}")]
    Connection(#[from] crate::connection::Error),

    #[error("Request error: {0}")]
    Request(#[from] RequestError),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("Server error {0:?} with message \"{1}\"")]
    ServerError(ProtocolError, String),

    #[error("All retries failed: {0}")]
    RetryFailed(#[from] crate::backoff::BackoffError),
}

impl Error {
    pub(crate) fn exactly_one_topic(len: usize) -> Self {
        Self::InvalidResponse(format!("Expected a single topic in response, got {len}"))
    }

    pub(crate) fn exactly_one_partition(len: usize) -> Self {
        Self::InvalidResponse(format!(
            "Expected a single partition in response, got {len}"
        ))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
