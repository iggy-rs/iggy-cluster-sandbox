use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;

#[derive(Debug, Default, PartialEq)]
pub struct Ping {}

impl Ping {
    pub fn new_command() -> Command {
        Command::Ping(Ping {})
    }
}

impl BytesSerializable for Ping {
    fn as_bytes(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Ping, SystemError> {
        if !bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let command = Ping {};
        Ok(command)
    }
}
