use std::array::TryFromSliceError;
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SystemError {
    #[error("Unhealthy cluster")]
    UnhealthyCluster,
    #[error("Invalid cluster secret")]
    InvalidClusterSecret,
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
    #[error("Invalid node with ID: {0}")]
    InvalidNode(u64),
    #[error("Cannot append message")]
    CannotAppendMessage,
    #[error("Cannot send command")]
    CannotSendCommand,
    #[error("Cannot read response")]
    CannotReadResponse,
    #[error("Received error response with status: {0}")]
    ErrorResponse(u32),
    #[error("Invalid offset")]
    InvalidOffset,
    #[error("Invalid count")]
    InvalidCount,
    #[error("Invalid stream ID")]
    InvalidStreamId,
    #[error("Vote rejected")]
    VoteRejected,
    #[error("Leader rejected")]
    LeaderRejected,
}

impl SystemError {
    pub fn as_code(&self) -> u32 {
        match self {
            SystemError::UnhealthyCluster => 1,
            SystemError::InvalidClusterSecret => 2,
            SystemError::IoError(_) => 3,
            SystemError::TryFromSliceError(_) => 4,
            SystemError::ConfigNotFound(_) => 5,
            SystemError::ConfigInvalid(_) => 6,
            SystemError::CannotConnectToClusterNode(_) => 7,
            SystemError::InvalidClusterNodeAddress(_) => 8,
            SystemError::InvalidCommandCode(_) => 9,
            SystemError::InvalidRequest => 10,
            SystemError::InvalidResponse => 11,
            SystemError::ClientDisconnected => 12,
            SystemError::SendRequestFailed => 13,
            SystemError::InvalidCommand => 14,
            SystemError::InvalidNode(_) => 15,
            SystemError::CannotAppendMessage => 16,
            SystemError::CannotSendCommand => 17,
            SystemError::CannotReadResponse => 18,
            SystemError::ErrorResponse(_) => 19,
            SystemError::InvalidOffset => 20,
            SystemError::InvalidCount => 21,
            SystemError::InvalidStreamId => 22,
            SystemError::VoteRejected => 23,
            SystemError::LeaderRejected => 24,
        }
    }
}
