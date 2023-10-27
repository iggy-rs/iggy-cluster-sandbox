use crate::error::SystemError;
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
}

impl Command {
    pub fn as_code(&self) -> u32 {
        match self {
            Command::Ping => 0x01,
        }
    }

    pub fn from_code(code: u32) -> Result<Command, SystemError> {
        match code {
            0x01 => Ok(Command::Ping),
            _ => Err(SystemError::InvalidCommandCode(code)),
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Command::Ping => vec![0x01],
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Ping => write!(f, "Ping"),
        }
    }
}
