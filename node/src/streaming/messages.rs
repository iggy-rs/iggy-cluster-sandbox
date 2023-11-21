use bytes::BufMut;
use sdk::bytes_serializable::BytesSerializable;
use sdk::error::SystemError;

#[derive(Debug)]
pub struct Message {
    pub offset: u64,
    pub id: u64,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn new(offset: u64, id: u64, payload: Vec<u8>) -> Self {
        Self {
            offset,
            id,
            payload,
        }
    }

    pub fn get_size(&self) -> u32 {
        20 + self.payload.len() as u32
    }
}

impl BytesSerializable for Message {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.put_u64_le(self.offset);
        bytes.put_u64_le(self.id);
        bytes.put_u32_le(self.payload.len() as u32);
        bytes.extend(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let offset = u64::from_le_bytes(bytes[0..8].try_into()?);
        let id = u64::from_le_bytes(bytes[8..16].try_into()?);
        let payload_length = u32::from_le_bytes(bytes[16..20].try_into()?) as usize;
        let payload = bytes[20..payload_length + 20].to_vec();
        Ok(Self {
            offset,
            id,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_serialize_and_deserialize_message() {
        let message = Message::new(1, 2, b"test".to_vec());
        let bytes = message.as_bytes();
        let deserialized_message = Message::from_bytes(&bytes).unwrap();
        assert_eq!(message.offset, deserialized_message.offset);
        assert_eq!(message.id, deserialized_message.id);
        assert_eq!(message.payload, deserialized_message.payload);
    }
}
