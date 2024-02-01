use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

pub const CREATE_STREAM_CODE: u32 = 31;

#[derive(Debug)]
pub struct CreateStream {
    pub id: u64,
    pub replication_factor: Option<u8>,
}

impl CreateStream {
    pub fn new(id: u64, replication_factor: Option<u8>) -> CreateStream {
        CreateStream {
            id,
            replication_factor,
        }
    }

    pub fn new_command(id: u64, replication_factor: Option<u8>) -> Command {
        Command::CreateStream(Self::new(id, replication_factor))
    }
}

impl BytesSerializable for CreateStream {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(9);
        bytes.put_u64_le(self.id);
        bytes.put_u8(self.replication_factor.unwrap_or(0));
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<CreateStream, SystemError> {
        if bytes.len() != 9 {
            return Err(SystemError::InvalidCommand);
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let replication_factor = bytes[8];
        let replication_factor = if replication_factor == 0 {
            None
        } else {
            Some(replication_factor)
        };
        let command = CreateStream {
            id,
            replication_factor,
        };
        Ok(command)
    }
}
