use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SystemError {
    #[error("IO error")]
    IoError(#[from] io::Error),
    #[error("Configuration not found: {0}.")]
    ConfigNotFound(String),
    #[error("Configuration not is invalid: {0}.")]
    ConfigInvalid(String),
}
