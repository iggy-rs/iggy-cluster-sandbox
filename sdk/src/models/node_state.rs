use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::BufMut;
use std::fmt::Display;

#[derive(Debug, Default)]
pub struct NodeState {
    pub id: u64,
    pub address: String,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
}

impl Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NodeState {{ id: {}, address: {}, term: {}, commit_index: {}, last_applied: {} }}",
            self.id, self.address, self.term, self.commit_index, self.last_applied
        )
    }
}

impl BytesSerializable for NodeState {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(40);
        bytes.put_u64_le(self.id);
        bytes.put_u8(self.address.len() as u8);
        bytes.put_slice(self.address.as_bytes());
        bytes.put_u64_le(self.term);
        bytes.put_u64_le(self.commit_index);
        bytes.put_u64_le(self.last_applied);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        if bytes.len() < 25 {
            return Err(SystemError::InvalidCommand);
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into()?);
        let address_length = bytes[8] as usize;
        let address = String::from_utf8(bytes[9..9 + address_length].to_vec()).unwrap();
        let term = u64::from_le_bytes(bytes[9 + address_length..17 + address_length].try_into()?);
        let commit_index =
            u64::from_le_bytes(bytes[17 + address_length..25 + address_length].try_into()?);
        let last_applied =
            u64::from_le_bytes(bytes[25 + address_length..33 + address_length].try_into()?);
        Ok(NodeState {
            id,
            address,
            term,
            commit_index,
            last_applied,
        })
    }
}
