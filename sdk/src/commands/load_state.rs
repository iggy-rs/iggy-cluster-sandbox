use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;

#[derive(Debug)]
pub struct LoadState {
    pub start_index: u64,
}

impl LoadState {
    pub fn new_command(start_index: u64) -> Command {
        Command::LoadState(LoadState { start_index })
    }
}

impl BytesSerializable for LoadState {
    fn as_bytes(&self) -> Vec<u8> {
        Vec::with_capacity(8)
    }

    fn from_bytes(bytes: &[u8]) -> Result<LoadState, SystemError> {
        if bytes.len() != 8 {
            return Err(SystemError::InvalidCommand);
        }

        let start_index = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let command = LoadState { start_index };
        Ok(command)
    }
}
