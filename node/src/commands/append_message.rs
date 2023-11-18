use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::BufMut;
use std::str::from_utf8;

#[derive(Debug, Default, PartialEq)]
pub struct AppendMessage {
    pub message: String,
}

impl BytesSerializable for AppendMessage {
    fn as_bytes(&self) -> Vec<u8> {
        let data_len = self.message.len();
        let mut bytes = Vec::with_capacity(4 + data_len);
        bytes.put_u32(data_len as u32);
        bytes.extend(self.message.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<AppendMessage, SystemError> {
        if bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let message_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let message =
            from_utf8(&bytes[4..=message_len]).map_err(|_| SystemError::InvalidCommand)?;

        let command = AppendMessage {
            message: message.to_string(),
        };
        Ok(command)
    }
}
