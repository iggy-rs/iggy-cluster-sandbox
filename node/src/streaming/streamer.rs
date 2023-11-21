use crate::bytes_serializable::BytesSerializable;
use crate::commands::append_messages::AppendMessages;
use crate::error::SystemError;
use crate::streaming::file;
use crate::streaming::messages::Message;
use std::fmt::{Display, Formatter};
use std::fs::create_dir_all;
use std::path::Path;
use tracing::{error, info};

const LOG_FILE: &str = "stream.log";

#[derive(Debug)]
pub struct Streamer {
    path: String,
    stream_path: String,
    messages: Vec<Message>,
    current_offset: u64,
    current_position: u64,
}

impl Streamer {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            stream_path: format!("{}/{}", path, LOG_FILE),
            messages: Vec::new(),
            current_offset: 0,
            current_position: 0,
        }
    }

    pub async fn init(&mut self) {
        if !Path::new(&self.path).exists() {
            create_dir_all(&self.path)
                .unwrap_or_else(|_| panic!("Failed to create stream directory: {}", self.path));
            info!("Created stream directory: {}", self.path);
        }

        if !Path::new(&self.stream_path).exists() {
            file::write(&self.stream_path)
                .await
                .unwrap_or_else(|_| panic!("Failed to create stream file: {}", self.stream_path));
            info!("Created empty stream file: {}", self.stream_path);
        } else {
            let (messages, position) = self.load_messages().await;
            if !messages.is_empty() {
                self.messages = messages;
                self.current_position = position;
                self.current_offset = self.messages.len() as u64 - 1;
            }
        }

        info!(
            "Initialized streamer, path: {}, messages count: {}",
            self.stream_path,
            self.messages.len()
        );
    }

    pub async fn append_messages(
        &mut self,
        append_messages: AppendMessages,
    ) -> Result<(), SystemError> {
        for message_to_append in append_messages.messages {
            if !self.messages.is_empty() {
                self.current_offset += 1;
            }

            let message = Message::new(
                self.current_offset,
                message_to_append.id,
                message_to_append.payload,
            );
            let size = message.get_size();
            let bytes = message.as_bytes();
            self.messages.push(message);
            let file = file::append(&self.stream_path)
                .await
                .unwrap_or_else(|_| panic!("Failed to open stream file: {}", self.stream_path));
            let result = file.write_all_at(bytes, self.current_position).await;
            if result.0.is_err() {
                error!(
                    "Failed to append message to stream file: {}",
                    self.stream_path
                );
                return Err(SystemError::CannotAppendMessage);
            }
            self.current_position += size as u64;
            if file.close().await.is_err() {
                error!("Failed to close stream file: {}", self.stream_path);
            }

            info!(
                "Appended message to stream file: {} at offset: {}, position: {}",
                self.stream_path, self.current_offset, self.current_position
            );
        }

        Ok(())
    }

    pub async fn load_messages(&self) -> (Vec<Message>, u64) {
        let mut messages = Vec::new();
        let file = file::open(&self.stream_path)
            .await
            .unwrap_or_else(|_| panic!("Failed to read stream file: {}", self.stream_path));

        let mut position = 0u64;
        loop {
            let buffer = vec![0u8; 8];
            let (result, buffer) = file.read_exact_at(buffer, position).await;
            if result.is_err() {
                break;
            }

            let offset = u64::from_le_bytes(buffer.try_into().unwrap());
            position += 8;

            let buffer = vec![0u8; 8];
            let (result, buffer) = file.read_exact_at(buffer, position).await;
            if result.is_err() {
                break;
            }
            let id = u64::from_le_bytes(buffer.try_into().unwrap());
            position += 8;

            let buffer = vec![0u8; 4];
            let payload_length = file.read_exact_at(buffer, position).await;
            if payload_length.0.is_err() {
                error!("Failed to read payload length");
                break;
            }

            let payload_length = u32::from_le_bytes(payload_length.1.try_into().unwrap());
            position += 4;
            let buffer = vec![0; payload_length as usize];
            let payload = file.read_exact_at(buffer, position).await;
            if payload.0.is_err() {
                error!("Failed to read payload");
                break;
            }

            position += payload_length as u64;
            let message = Message::new(offset, id, payload.1);
            messages.push(message);
        }

        (messages, position)
    }
}

impl Display for Streamer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Streamer {{ path: {}, messages: {} }}",
            self.path,
            self.messages.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_dir_all;

    const BASE_DIR: &str = "local_data";

    struct Test {}

    impl Test {
        fn streams_path(&self) -> String {
            format!("{BASE_DIR}/test_streams")
        }
    }

    impl Drop for Test {
        fn drop(&mut self) {
            let _ = remove_dir_all(BASE_DIR);
        }
    }

    #[monoio::test]
    async fn messages_should_be_stored_on_disk() {
        let test = Test {};
        let mut streamer = Streamer::new(&test.streams_path());
        streamer.init().await;
        let append_messages = AppendMessages {
            messages: vec![
                crate::commands::append_messages::Message {
                    id: 1,
                    payload: b"message-1".to_vec(),
                },
                crate::commands::append_messages::Message {
                    id: 2,
                    payload: b"message-2".to_vec(),
                },
                crate::commands::append_messages::Message {
                    id: 3,
                    payload: b"message-3".to_vec(),
                },
            ],
        };
        let result = streamer.append_messages(append_messages).await;
        assert!(result.is_ok());
        let (loaded_messages, position) = streamer.load_messages().await;
        assert!(position > 0);
        assert_eq!(loaded_messages.len(), 3);
        let loaded_message1 = &loaded_messages[0];
        let loaded_message2 = &loaded_messages[1];
        let loaded_message3 = &loaded_messages[2];
        assert_message(loaded_message1, 0, 1, b"message-1");
        assert_message(loaded_message2, 1, 2, b"message-2");
        assert_message(loaded_message3, 2, 3, b"message-3");
    }

    fn assert_message(message: &Message, offset: u64, id: u64, payload: &[u8]) {
        assert_eq!(message.offset, offset);
        assert_eq!(message.id, id);
        assert_eq!(message.payload, payload);
    }
}
