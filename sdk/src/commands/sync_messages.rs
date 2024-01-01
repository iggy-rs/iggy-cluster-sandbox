use crate::bytes_serializable::BytesSerializable;
use crate::commands::append_messages::AppendableMessage;
use crate::commands::command::Command;
use crate::error::SystemError;
use bytes::BufMut;

#[derive(Debug)]
pub struct SyncMessages {
    pub term: u64,
    pub stream_id: u64,
    pub messages: Vec<AppendableMessage>,
}

impl SyncMessages {
    pub fn new_command(term: u64, stream_id: u64, messages: Vec<AppendableMessage>) -> Command {
        Command::SyncMessages(SyncMessages {
            term,
            stream_id,
            messages,
        })
    }
}

impl BytesSerializable for SyncMessages {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.put_u64_le(self.term);
        bytes.put_u64_le(self.stream_id);
        for message in &self.messages {
            bytes.extend(&message.as_bytes());
        }
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<SyncMessages, SystemError> {
        if bytes.is_empty() {
            return Err(SystemError::InvalidCommand);
        }

        let term = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let stream_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let payload = &bytes[16..];
        let mut messages = Vec::new();
        let mut position = 0;
        while position < payload.len() {
            let message = AppendableMessage::from_bytes(&payload[position..])?;
            position += 12 + message.payload.len();
            messages.push(message);
        }

        Ok(SyncMessages {
            term,
            stream_id,
            messages,
        })
    }
}
