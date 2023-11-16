use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::BufMut;
use std::str::from_utf8;

#[derive(Debug, Default, PartialEq)]
pub struct AppendData {
    pub data: String,
}

impl BytesSerializable for AppendData {
    fn as_bytes(&self) -> Vec<u8> {
        let data_len = self.data.len();
        let mut bytes = Vec::with_capacity(4 + data_len);
        bytes.put_u32(data_len as u32);
        bytes.extend(self.data.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<AppendData, SystemError> {
        if bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let data_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let data = from_utf8(&bytes[4..=data_len]).map_err(|_| SystemError::InvalidCommand)?;

        let command = AppendData {
            data: data.to_string(),
        };
        Ok(command)
    }
}
