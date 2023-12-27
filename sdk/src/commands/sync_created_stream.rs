use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

#[derive(Debug)]
pub struct SyncCreatedStream {
    pub term: u64,
    pub stream_id: u64,
}

impl SyncCreatedStream {
    pub fn new_command(term: u64, stream_id: u64) -> Command {
        Command::SyncCreatedStream(SyncCreatedStream { term, stream_id })
    }
}

impl BytesSerializable for SyncCreatedStream {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16);
        bytes.put_u64_le(self.term);
        bytes.put_u64_le(self.stream_id);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<SyncCreatedStream, SystemError> {
        if bytes.len() != 16 {
            return Err(SystemError::InvalidCommand);
        }

        let term = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let stream_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        Ok(SyncCreatedStream { term, stream_id })
    }
}
