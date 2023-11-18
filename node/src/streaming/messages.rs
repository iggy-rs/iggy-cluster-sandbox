use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::BufMut;
use std::str::from_utf8;

#[derive(Debug)]
pub struct Message {
    pub offset: u64,
    pub value: String,
}

impl Message {
    pub fn new(offset: u64, value: String) -> Self {
        Self { offset, value }
    }

    pub fn get_size(&self) -> u32 {
        12 + self.value.len() as u32
    }
}

impl BytesSerializable for Message {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.put_u64_le(self.offset);
        bytes.put_u32_le(self.value.len() as u32);
        bytes.extend(self.value.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let offset = u64::from_le_bytes(bytes[0..8].try_into()?);
        let value_len = u32::from_le_bytes(bytes[8..12].try_into()?);
        let value = from_utf8(&bytes[12..12 + value_len as usize])
            .unwrap()
            .to_string();
        Ok(Self { offset, value })
    }
}
