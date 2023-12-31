use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;

#[derive(Debug)]
pub struct GetStreams {}

impl GetStreams {
    pub fn new_command() -> Command {
        Command::GetStreams(GetStreams {})
    }
}

impl BytesSerializable for GetStreams {
    fn as_bytes(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }

    fn from_bytes(bytes: &[u8]) -> Result<GetStreams, SystemError> {
        if !bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let command = GetStreams {};
        Ok(command)
    }
}
