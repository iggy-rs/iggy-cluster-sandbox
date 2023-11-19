use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::BufMut;

#[derive(Debug)]
pub struct Message {
    pub offset: u64,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn new(offset: u64, payload: Vec<u8>) -> Self {
        Self { offset, payload }
    }

    pub fn get_size(&self) -> u32 {
        12 + self.payload.len() as u32
    }
}

impl BytesSerializable for Message {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.put_u64_le(self.offset);
        bytes.put_u32_le(self.payload.len() as u32);
        bytes.extend(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let offset = u64::from_le_bytes(bytes[0..8].try_into()?);
        let payload_length = u32::from_le_bytes(bytes[8..12].try_into()?);
        let payload = bytes[12..12 + payload_length as usize].to_vec();
        Ok(Self { offset, payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_serialize_and_deserialize_message() {
        let message = Message::new(1, b"test".to_vec());
        let bytes = message.as_bytes();
        let deserialized_message = Message::from_bytes(&bytes).unwrap();
        assert_eq!(message.offset, deserialized_message.offset);
        assert_eq!(message.payload, deserialized_message.payload);
    }
}
