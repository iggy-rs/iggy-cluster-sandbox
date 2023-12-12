use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

#[derive(Debug)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: u64,
}

impl RequestVote {
    pub fn new_command(term: u64, candidate_id: u64) -> Command {
        Command::RequestVote(RequestVote { term, candidate_id })
    }
}

impl BytesSerializable for RequestVote {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16);
        bytes.put_u64_le(self.term);
        bytes.put_u64_le(self.candidate_id);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError> {
        if bytes.len() != 16 {
            return Err(SystemError::InvalidCommand);
        }
        let term = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let candidate_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        Ok(RequestVote { term, candidate_id })
    }
}
