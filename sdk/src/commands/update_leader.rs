use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

#[derive(Debug)]
pub struct UpdateLeader {
    pub term: u64,
}

impl UpdateLeader {
    pub fn new_command(term: u64) -> Command {
        Command::UpdateLeader(UpdateLeader { term })
    }
}

impl BytesSerializable for UpdateLeader {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16);
        bytes.put_u64_le(self.term);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError> {
        if bytes.len() != 8 {
            return Err(SystemError::InvalidCommand);
        }
        let term = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        Ok(UpdateLeader { term })
    }
}
