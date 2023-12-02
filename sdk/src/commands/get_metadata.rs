use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;

#[derive(Debug)]
pub struct GetMetadata {}

impl GetMetadata {
    pub fn new_command() -> Command {
        Command::GetMetadata(GetMetadata {})
    }
}

impl BytesSerializable for GetMetadata {
    fn as_bytes(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }

    fn from_bytes(bytes: &[u8]) -> Result<GetMetadata, SystemError> {
        if !bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let command = GetMetadata {};
        Ok(command)
    }
}
