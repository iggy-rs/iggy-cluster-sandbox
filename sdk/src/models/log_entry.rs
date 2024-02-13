use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::{BufMut, Bytes};
use std::fmt::Display;

#[derive(Debug)]
pub struct LogEntry {
    pub index: u64,
    pub size: u32,
    pub data: Bytes,
}

impl Display for LogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "index: {}, size: {}, data: {:?}",
            self.index, self.size, self.data
        )
    }
}

impl BytesSerializable for LogEntry {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12 + self.data.len());
        bytes.put_u64_le(self.index);
        bytes.put_u32_le(self.data.len() as u32);
        bytes.extend(&self.data);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        if bytes.len() < 12 {
            return Err(SystemError::InvalidCommand);
        }

        let index = u64::from_le_bytes(bytes[0..8].try_into()?);
        let size = u32::from_le_bytes(bytes[8..12].try_into()?);
        let data = Bytes::copy_from_slice(&bytes[12..12 + size as usize]);
        Ok(LogEntry { index, size, data })
    }
}
