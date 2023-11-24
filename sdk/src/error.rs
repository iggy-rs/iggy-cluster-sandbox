use std::array::TryFromSliceError;
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SystemError {
    #[error("Unhealthy cluster")]
    UnhealthyCluster,
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
    #[error("Invalid command")]
    InvalidCommand,
    #[error("Invalid node: {0}")]
    InvalidNode(String),
    #[error("Cannot append message")]
    CannotAppendMessage,
    #[error("Cannot send command")]
    CannotSendCommand,
    #[error("Cannot read response")]
    CannotReadResponse,
    #[error("Received error response with status: {0}")]
    ErrorResponse(u32),
}

impl SystemError {
    pub fn as_code(&self) -> u32 {
        match self {
            SystemError::UnhealthyCluster => 1,
            SystemError::IoError(_) => 2,
            SystemError::TryFromSliceError(_) => 3,
            SystemError::ConfigNotFound(_) => 4,
            SystemError::ConfigInvalid(_) => 5,
            SystemError::CannotConnectToClusterNode(_) => 6,
            SystemError::InvalidClusterNodeAddress(_) => 7,
            SystemError::InvalidCommandCode(_) => 8,
            SystemError::InvalidRequest => 9,
            SystemError::InvalidResponse => 10,
            SystemError::ClientDisconnected => 11,
            SystemError::SendRequestFailed => 12,
            SystemError::InvalidCommand => 13,
            SystemError::InvalidNode(_) => 14,
            SystemError::CannotAppendMessage => 15,
            SystemError::CannotSendCommand => 16,
            SystemError::CannotReadResponse => 17,
            SystemError::ErrorResponse(_) => 18,
        }
    }
}
