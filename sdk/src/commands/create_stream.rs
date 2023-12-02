use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;
use std::str::from_utf8;

#[derive(Debug)]
pub struct CreateStream {
    pub id: u64,
    pub name: String,
}

impl CreateStream {
    pub fn new_command(id: u64, name: String) -> Command {
        Command::CreateStream(CreateStream { id, name })
    }
}

impl BytesSerializable for CreateStream {
    fn as_bytes(&self) -> Vec<u8> {
        let name_bytes = self.name.as_bytes();
        let mut bytes = Vec::with_capacity(8 + 4 + name_bytes.len());
        bytes.put_u64_le(self.id);
        bytes.put_u32_le(name_bytes.len() as u32);
        bytes.extend(name_bytes);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<CreateStream, SystemError> {
        if bytes.len() < 12 {
            return Err(SystemError::InvalidCommand);
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let name_length = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let name =
            from_utf8(&bytes[12..name_length + 12]).map_err(|_| SystemError::InvalidCommand)?;
        let command = CreateStream {
            id,
            name: name.to_string(),
        };
        Ok(command)
    }
}
