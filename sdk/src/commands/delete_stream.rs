use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

pub const DELETE_STREAM_CODE: u32 = 32;

#[derive(Debug)]
pub struct DeleteStream {
    pub id: u64,
}

impl DeleteStream {
    pub fn new(id: u64) -> DeleteStream {
        DeleteStream { id }
    }

    pub fn new_command(id: u64) -> Command {
        Command::DeleteStream(Self::new(id))
    }
}

impl BytesSerializable for DeleteStream {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8);
        bytes.put_u64_le(self.id);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<DeleteStream, SystemError> {
        if bytes.len() != 8 {
            return Err(SystemError::InvalidCommand);
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let command = DeleteStream { id };
        Ok(command)
    }
}
