use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

#[derive(Debug)]
pub struct PollMessages {
    pub offset: u64,
    pub count: u64,
}

impl PollMessages {
    pub fn new_command(offset: u64, count: u64) -> Command {
        Command::PollMessages(PollMessages { offset, count })
    }
}

impl BytesSerializable for PollMessages {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12);
        bytes.put_u64_le(self.offset);
        bytes.put_u64_le(self.count);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError> {
        if bytes.len() != 16 {
            return Err(SystemError::InvalidCommand);
        }
        let offset = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        Ok(PollMessages { offset, count })
    }
}
