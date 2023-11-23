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
    #[error("Invalid command")]
    InvalidCommand,
    #[error("Invalid node: {0}")]
    InvalidNode(String),
    #[error("Cannot append message")]
    CannotAppendMessage,
    #[error("Unhealthy cluster")]
    UnhealthyCluster,
}

impl SystemError {
    pub fn as_code(&self) -> u32 {
        match self {
            SystemError::IoError(_) => 1,
            SystemError::TryFromSliceError(_) => 2,
            SystemError::ConfigNotFound(_) => 3,
            SystemError::ConfigInvalid(_) => 4,
            SystemError::CannotConnectToClusterNode(_) => 5,
            SystemError::InvalidClusterNodeAddress(_) => 6,
            SystemError::InvalidCommandCode(_) => 7,
            SystemError::InvalidRequest => 8,
            SystemError::InvalidResponse => 9,
            SystemError::ClientDisconnected => 10,
            SystemError::SendRequestFailed => 11,
            SystemError::InvalidCommand => 12,
            SystemError::InvalidNode(_) => 13,
            SystemError::CannotAppendMessage => 14,
            SystemError::UnhealthyCluster => 100,
        }
    }
}
