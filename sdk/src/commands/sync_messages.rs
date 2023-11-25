use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

#[derive(Debug)]
pub struct SyncMessages {
    pub messages: Vec<Message>,
}

#[derive(Debug)]
pub struct Message {
    pub offset: u64,
    pub id: u64,
    pub payload: Vec<u8>,
}

impl SyncMessages {
    pub fn new_command(messages: Vec<Message>) -> Command {
        Command::SyncMessages(SyncMessages { messages })
    }
}

impl BytesSerializable for Message {
    fn as_bytes(&self) -> Vec<u8> {
        let payload_length = self.payload.len();
        let mut bytes = Vec::with_capacity(20 + payload_length);
        bytes.put_u64_le(self.offset);
        bytes.put_u64_le(self.id);
        bytes.put_u32_le(payload_length as u32);
        bytes.extend(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError> {
        if bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }
        let offset = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let payload_length = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let payload = bytes[20..payload_length + 20].to_vec();
        Ok(Message {
            offset,
            id,
            payload,
        })
    }
}

impl BytesSerializable for SyncMessages {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for message in &self.messages {
            bytes.extend(&message.as_bytes());
        }
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<SyncMessages, SystemError> {
        if bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let mut messages = Vec::new();
        let mut position = 0;
        while position < bytes.len() {
            let message = Message::from_bytes(&bytes[position..])?;
            position += 20 + message.payload.len();
            messages.push(message);
        }

        Ok(SyncMessages { messages })
    }
}
