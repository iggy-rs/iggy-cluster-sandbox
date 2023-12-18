use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;
use std::str::from_utf8;

#[derive(Debug, Default, PartialEq)]
pub struct Hello {
    pub secret: String,
    pub name: String,
    pub node_id: u64,
    pub term: u64,
    pub leader_id: Option<u64>,
}

impl Hello {
    pub fn new_command(
        secret: String,
        name: String,
        node_id: u64,
        term: u64,
        leader_id: Option<u64>,
    ) -> Command {
        Command::Hello(Hello {
            secret,
            name,
            node_id,
            term,
            leader_id,
        })
    }
}

impl BytesSerializable for Hello {
    fn as_bytes(&self) -> Vec<u8> {
        let secret_len = self.secret.len();
        let name_len = self.name.len();
        let mut bytes = Vec::with_capacity(2 + secret_len + name_len);
        bytes.put_u8(secret_len as u8);
        bytes.extend(self.secret.as_bytes());
        bytes.put_u8(name_len as u8);
        bytes.extend(self.name.as_bytes());
        bytes.put_u64_le(self.node_id);
        bytes.put_u64_le(self.term);
        bytes.put_u64_le(self.leader_id.unwrap_or(0));
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Hello, SystemError> {
        if bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let secret_len = bytes[0] as usize;
        let secret = from_utf8(&bytes[1..=secret_len]).map_err(|_| SystemError::InvalidCommand)?;
        let name_len = bytes[secret_len + 1] as usize;
        let name = from_utf8(&bytes[secret_len + 2..=secret_len + 1 + name_len])
            .map_err(|_| SystemError::InvalidCommand)?;
        let node_id = u64::from_le_bytes(
            bytes[secret_len + name_len + 2..=secret_len + name_len + 9]
                .try_into()
                .map_err(|_| SystemError::InvalidCommand)?,
        );
        let term = u64::from_le_bytes(
            bytes[secret_len + name_len + 10..=secret_len + name_len + 17]
                .try_into()
                .map_err(|_| SystemError::InvalidCommand)?,
        );
        let leader_id = u64::from_le_bytes(
            bytes[secret_len + name_len + 18..=secret_len + name_len + 25]
                .try_into()
                .map_err(|_| SystemError::InvalidCommand)?,
        );
        let leader_id = if leader_id == 0 {
            None
        } else {
            Some(leader_id)
        };

        let command = Hello {
            secret: secret.to_string(),
            name: name.to_string(),
            node_id,
            term,
            leader_id,
        };
        Ok(command)
    }
}
