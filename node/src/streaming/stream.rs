use crate::streaming::file;
use bytes::Bytes;
use sdk::bytes_serializable::BytesSerializable;
use sdk::commands::append_messages::AppendableMessage;
use sdk::error::SystemError;
use sdk::models::message::Message;
use std::fmt::{Display, Formatter};
use std::fs::create_dir_all;
use std::path::Path;
use tracing::{error, info};

const EMPTY_MESSAGES: &[Message] = &[];
const LOG_FILE: &str = "stream.log";

#[derive(Debug)]
pub(crate) struct Stream {
    pub stream_id: u64,
    pub leader_id: u64,
    pub directory_path: String,
    pub log_path: String,
    pub messages: Vec<Message>,
    pub current_offset: u64,
    pub current_position: u64,
    pub current_id: u64,
}

impl Display for Stream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stream {{ id: {}, path: {}, messages: {} }}",
            self.stream_id,
            self.log_path,
            self.messages.len()
        )
    }
}

impl Stream {
    pub fn new(stream_id: u64, leader_id: u64, path: &str) -> Self {
        let directory_path = format!("{path}/{stream_id}");
        Self {
            stream_id,
            leader_id,
            log_path: format!("{directory_path}/{LOG_FILE}"),
            directory_path,
            messages: Vec::new(),
            current_offset: 0,
            current_position: 0,
            current_id: 0,
        }
    }

    pub async fn init(&mut self) {
        if !Path::new(&self.directory_path).exists() {
            create_dir_all(&self.directory_path).unwrap_or_else(|_| {
                panic!("Failed to create stream directory: {}", self.directory_path)
            });
            info!("Created stream directory: {}", self.directory_path);
        }

        if !Path::new(&self.log_path).exists() {
            file::write(&self.log_path)
                .await
                .unwrap_or_else(|_| panic!("Failed to create stream file: {}", self.log_path));
            info!("Created empty stream file: {}", self.log_path);
        } else {
            let (messages, position) = self.load_messages_from_disk().await;
            if !messages.is_empty() {
                self.messages = messages;
                self.current_position = position;
                self.current_offset = self.messages.len() as u64 - 1;
                self.current_id = self.messages.iter().max_by_key(|m| m.id).unwrap().id;
            }
        }

        info!(
            "Initialized stream with ID: {}, path: {}, messages count: {}",
            self.stream_id,
            self.log_path,
            self.messages.len()
        );
    }

    pub fn delete(&self) {
        if !Path::new(&self.directory_path).exists() {
            error!("Stream with ID: {} does not exist", self.stream_id);
        }

        if std::fs::remove_dir_all(&self.directory_path).is_err() {
            error!("Failed to delete stream with ID: {}", self.stream_id);
            return;
        }

        info!("Deleted stream with ID: {}", self.stream_id);
    }

    pub async fn append_messages(
        &mut self,
        messages: &[AppendableMessage],
    ) -> Result<Vec<Message>, SystemError> {
        let mut uncommitted_messages = Vec::with_capacity(messages.len());
        for message_to_append in messages {
            if !self.messages.is_empty() || !uncommitted_messages.is_empty() {
                self.current_offset += 1;
            }

            if message_to_append.id == 0 {
                self.current_id += 1;
            } else {
                self.current_id = message_to_append.id;
            }

            let message = Message::new(
                self.current_offset,
                self.current_id,
                message_to_append.payload.clone(),
            );
            uncommitted_messages.push(message);
        }

        Ok(uncommitted_messages)
    }

    pub async fn commit_messages(&mut self, messages: Vec<Message>) -> Result<u64, SystemError> {
        for message in messages {
            let size = message.get_size();
            let bytes = message.as_bytes();
            let file = file::append(&self.log_path)
                .await
                .unwrap_or_else(|_| panic!("Failed to open stream file: {}", &self.log_path));
            let result = file.write_all_at(bytes, self.current_position).await;
            if result.0.is_err() {
                error!(
                    "Failed to append message to stream file: {}",
                    &self.log_path
                );
                return Err(SystemError::CannotAppendMessage);
            }
            self.current_position += size as u64;
            if file.close().await.is_err() {
                error!("Failed to close stream file: {}", &self.log_path);
            }

            info!(
                "Appended message to stream file: {} at offset: {}, position: {}",
                &self.log_path, self.current_offset, self.current_position
            );

            self.messages.push(message);
        }
        Ok(self.current_offset)
    }

    pub fn poll_messages(&self, offset: u64, count: u64) -> Result<&[Message], SystemError> {
        if self.messages.is_empty() {
            return Ok(EMPTY_MESSAGES);
        }

        if offset > self.current_offset {
            return Err(SystemError::InvalidOffset);
        }

        if count == 0 {
            return Err(SystemError::InvalidCount);
        }

        let start_offset = offset;
        let mut end_offset = offset + count - 1;
        if end_offset > self.current_offset {
            end_offset = self.current_offset;
        }

        let messages = &self.messages[start_offset as usize..=end_offset as usize];
        Ok(messages)
    }

    pub async fn load_messages_from_disk(&self) -> (Vec<Message>, u64) {
        let mut messages = Vec::new();
        let file = file::open(&self.log_path)
            .await
            .unwrap_or_else(|_| panic!("Failed to read stream file: {}", self.log_path));

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
            let message = Message::new(offset, id, Bytes::from(payload.1));
            messages.push(message);
        }

        (messages, position)
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
        let stream_id = 1;
        let node_id = 2;
        let mut stream = Stream::new(stream_id, node_id, &test.streams_path());
        stream.init().await;
        let messages = vec![
            sdk::commands::append_messages::AppendableMessage {
                id: 1,
                payload: Bytes::from("message-1"),
            },
            sdk::commands::append_messages::AppendableMessage {
                id: 2,
                payload: Bytes::from("message-2"),
            },
            sdk::commands::append_messages::AppendableMessage {
                id: 3,
                payload: Bytes::from("message-3"),
            },
        ];
        let result = stream.append_messages(&messages).await;
        assert!(result.is_ok());

        let uncommitted_messages = result.unwrap();
        stream.commit_messages(uncommitted_messages).await.unwrap();

        let polled_messages = stream.poll_messages(0, 1000);
        assert!(polled_messages.is_ok());
        let polled_messages = polled_messages.unwrap();
        assert_eq!(polled_messages.len(), 3);
        let polled_message1 = &polled_messages[0];
        let polled_message2 = &polled_messages[1];
        let polled_message3 = &polled_messages[2];
        assert_message(polled_message1, 0, 1, b"message-1");
        assert_message(polled_message2, 1, 2, b"message-2");
        assert_message(polled_message3, 2, 3, b"message-3");

        let (loaded_messages, position) = stream.load_messages_from_disk().await;
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
