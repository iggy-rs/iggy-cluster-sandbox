use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::BufMut;

#[derive(Debug, Default, PartialEq)]
pub struct AppendMessage {
    pub payload: Vec<u8>,
}

impl BytesSerializable for AppendMessage {
    fn as_bytes(&self) -> Vec<u8> {
        let payload_length = self.payload.len();
        let mut bytes = Vec::with_capacity(4 + payload_length);
        bytes.put_u32_le(payload_length as u32);
        bytes.extend(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<AppendMessage, SystemError> {
        if bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let payload_length = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let payload = bytes[4..payload_length + 4].to_vec();
        let command = AppendMessage { payload };
        Ok(command)
    }
}
