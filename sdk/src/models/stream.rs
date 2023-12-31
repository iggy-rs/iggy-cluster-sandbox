use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::BufMut;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Stream {
    pub id: u64,
    pub offset: u64,
}

impl Display for Stream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stream {{ id: {}, offset: {} }}", self.id, self.offset)
    }
}

impl BytesSerializable for Stream {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16);
        bytes.put_u64_le(self.id);
        bytes.put_u64_le(self.offset);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        if bytes.len() != 16 {
            return Err(SystemError::InvalidCommand);
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        Ok(Stream { id, offset })
    }
}
