use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::{BufMut, Bytes};

#[derive(Debug)]
pub struct LogEntry {
    pub index: u64,
    pub data: Bytes,
}

impl BytesSerializable for LogEntry {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + self.data.len());
        bytes.put_u64_le(self.index);
        bytes.extend(&self.data);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let index = u64::from_le_bytes(bytes[0..8].try_into()?);
        let data = Bytes::copy_from_slice(&bytes[8..]);
        Ok(LogEntry { index, data })
    }
}
