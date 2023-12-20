use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

#[derive(Debug, Default, PartialEq)]
pub struct Heartbeat {
    pub term: u64,
    pub leader_id: Option<u64>,
}

impl Heartbeat {
    pub fn new_command(term: u64, leader_id: Option<u64>) -> Command {
        Command::Heartbeat(Heartbeat { term, leader_id })
    }
}

impl BytesSerializable for Heartbeat {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16);
        bytes.put_u64_le(self.term);
        bytes.put_u64_le(self.leader_id.unwrap_or(0));
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Heartbeat, SystemError> {
        if bytes.len() != 16 {
            return Err(SystemError::InvalidCommand);
        }

        let term = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let leader_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let leader_id = if leader_id == 0 {
            None
        } else {
            Some(leader_id)
        };
        let command = Heartbeat { term, leader_id };
        Ok(command)
    }
}
