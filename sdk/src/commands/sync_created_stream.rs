use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;

#[derive(Debug)]
pub struct SyncCreatedStream {
    pub id: u64,
}

impl SyncCreatedStream {
    pub fn new_command(id: u64) -> Command {
        Command::SyncCreatedStream(SyncCreatedStream { id })
    }
}

impl BytesSerializable for SyncCreatedStream {
    fn as_bytes(&self) -> Vec<u8> {
        self.id.to_le_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<SyncCreatedStream, SystemError> {
        if bytes.len() != 8 {
            return Err(SystemError::InvalidCommand);
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        Ok(SyncCreatedStream { id })
    }
}
