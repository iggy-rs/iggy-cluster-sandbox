use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

#[derive(Debug)]
pub struct AppendMessages {
    pub messages: Vec<Message>,
}

#[derive(Debug)]
pub struct Message {
    pub id: u64,
    pub payload: Vec<u8>,
}

impl AppendMessages {
    pub fn new_command(messages: Vec<Message>) -> Command {
        Command::AppendMessages(AppendMessages { messages })
    }
}

impl BytesSerializable for Message {
    fn as_bytes(&self) -> Vec<u8> {
        let payload_length = self.payload.len();
        let mut bytes = Vec::with_capacity(12 + payload_length);
        bytes.put_u64_le(self.id);
        bytes.put_u32_le(payload_length as u32);
        bytes.extend(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError> {
        if bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }
        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let payload_length = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let payload = bytes[12..payload_length + 12].to_vec();
        Ok(Message { id, payload })
    }
}

impl BytesSerializable for AppendMessages {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for message in &self.messages {
            bytes.extend(&message.as_bytes());
        }
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<AppendMessages, SystemError> {
        if bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let mut messages = Vec::new();
        let mut position = 0;
        while position < bytes.len() {
            let message = Message::from_bytes(&bytes[position..])?;
            position += 12 + message.payload.len();
            messages.push(message);
        }

        Ok(AppendMessages { messages })
    }
}
