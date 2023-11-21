use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;
use std::str::from_utf8;

#[derive(Debug, Default, PartialEq)]
pub struct Hello {
    pub name: String,
}

impl Hello {
    pub fn new_command(name: String) -> Command {
        Command::Hello(Hello { name })
    }
}

impl BytesSerializable for Hello {
    fn as_bytes(&self) -> Vec<u8> {
        let name_len = self.name.len();
        let mut bytes = Vec::with_capacity(1 + name_len);
        bytes.put_u8(name_len as u8);
        bytes.extend(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Hello, SystemError> {
        if bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let name_len = bytes[0] as usize;
        let name = from_utf8(&bytes[1..=name_len]).map_err(|_| SystemError::InvalidCommand)?;

        let command = Hello {
            name: name.to_string(),
        };
        Ok(command)
    }
}
