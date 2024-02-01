use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::BufMut;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Stream {
    pub id: u64,
    pub offset: u64,
    pub replication_factor: u8,
}

impl Display for Stream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stream {{ id: {}, offset: {}, replication_factor: {} }}",
            self.id, self.offset, self.replication_factor
        )
    }
}

impl BytesSerializable for Stream {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(17);
        bytes.put_u64_le(self.id);
        bytes.put_u64_le(self.offset);
        bytes.put_u8(self.replication_factor);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        if bytes.len() != 17 {
            return Err(SystemError::InvalidCommand);
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let replication_factor = bytes[16];
        Ok(Stream {
            id,
            offset,
            replication_factor,
        })
    }
}
