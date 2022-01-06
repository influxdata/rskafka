use thiserror::Error;

use crate::protocol::error::Error as ProtocolError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Connection error: {0}")]
    Connection(#[from] crate::connection::Error),

    #[error("Request error: {0}")]
    Request(#[from] crate::messenger::RequestError),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("Server error {0:?} with message \"{1}\"")]
    ServerError(ProtocolError, String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
