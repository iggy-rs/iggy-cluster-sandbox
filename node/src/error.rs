use std::array::TryFromSliceError;
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SystemError {
    #[error("IO error")]
    IoError(#[from] io::Error),
    #[error("Try from slice error")]
    TryFromSliceError(#[from] TryFromSliceError),
    #[error("Configuration not found: {0}.")]
    ConfigNotFound(String),
    #[error("Configuration not is invalid: {0}.")]
    ConfigInvalid(String),
    #[error("Cannot connect to cluster node: {0}.")]
    CannotConnectToClusterNode(String),
    #[error("Invalid cluster node address: {0}.")]
    InvalidClusterNodeAddress(String),
    #[error("Invalid command code: {0}.")]
    InvalidCommandCode(u32),
    #[error("Invalid request")]
    InvalidRequest,
    #[error("Invalid response")]
    InvalidResponse,
    #[error("Client disconnected")]
    ClientDisconnected,
    #[error("Send request failed")]
    SendRequestFailed,
}
