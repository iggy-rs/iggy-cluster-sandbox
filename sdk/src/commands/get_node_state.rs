use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;

#[derive(Debug)]
pub struct GetNodeState {}

impl GetNodeState {
    pub fn new_command() -> Command {
        Command::GetNodeState(GetNodeState {})
    }
}

impl BytesSerializable for GetNodeState {
    fn as_bytes(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }

    fn from_bytes(bytes: &[u8]) -> Result<GetNodeState, SystemError> {
        if !bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let command = GetNodeState {};
        Ok(command)
    }
}
