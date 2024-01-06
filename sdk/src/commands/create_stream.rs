use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

pub const CREATE_STREAM_CODE: u32 = 31;

#[derive(Debug)]
pub struct CreateStream {
    pub id: u64,
}

impl CreateStream {
    pub fn new(id: u64) -> CreateStream {
        CreateStream { id }
    }

    pub fn new_command(id: u64) -> Command {
        Command::CreateStream(Self::new(id))
    }
}

impl BytesSerializable for CreateStream {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8);
        bytes.put_u64_le(self.id);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<CreateStream, SystemError> {
        if bytes.len() != 8 {
            return Err(SystemError::InvalidCommand);
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let command = CreateStream { id };
        Ok(command)
    }
}
