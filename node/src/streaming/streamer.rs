use crate::streaming::file;
use crate::streaming::messages::Message;
use bytes::Bytes;
use sdk::bytes_serializable::BytesSerializable;
use sdk::commands::append_messages::AppendMessages;
use sdk::error::SystemError;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::create_dir_all;
use std::path::Path;
use tracing::{error, info, warn};

const EMPTY_MESSAGES: &[Message] = &[];
const LOG_FILE: &str = "stream.log";

#[derive(Debug)]
pub(crate) struct Streamer {
    path: String,
    streams: HashMap<u64, Stream>,
}

#[derive(Debug)]
pub(crate) struct Stream {
    id: u64,
    directory_path: String,
    log_path: String,
    messages: Vec<Message>,
    current_offset: u64,
    current_position: u64,
    current_id: u64,
}

impl Stream {
    pub fn new(id: u64, path: &str) -> Self {
        let directory_path = format!("{path}/{id}");
        Self {
            id,
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
            self.id,
            self.log_path,
            self.messages.len()
        );
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

impl Streamer {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            streams: HashMap::new(),
        }
    }

    pub async fn create_stream(&mut self, id: u64) {
        if self.streams.contains_key(&id) {
            warn!("Stream: {id} already exists.");
            return;
        }

        let mut stream = Stream::new(id, &self.path);
        stream.init().await;
        self.streams.insert(id, stream);
    }

    pub async fn init(&mut self) {
        for stream in self.streams.values_mut() {
            stream.init().await;
        }
    }

    pub async fn append_messages(
        &mut self,
        append_messages: &AppendMessages,
    ) -> Result<(), SystemError> {
        let stream = self.streams.get_mut(&append_messages.stream_id);
        if stream.is_none() {
            return Err(SystemError::InvalidStreamId);
        }

        let stream = stream.unwrap();
        for message_to_append in &append_messages.messages {
            if !stream.messages.is_empty() {
                stream.current_offset += 1;
            }

            if message_to_append.id == 0 {
                stream.current_id += 1;
            } else {
                stream.current_id = message_to_append.id;
            }

            let message = Message::new(
                stream.current_offset,
                stream.current_id,
                message_to_append.payload.clone(),
            );
            let size = message.get_size();
            let bytes = message.as_bytes();
            stream.messages.push(message);
            let file = file::append(&stream.log_path)
                .await
                .unwrap_or_else(|_| panic!("Failed to open stream file: {}", &stream.log_path));
            let result = file.write_all_at(bytes, stream.current_position).await;
            if result.0.is_err() {
                error!(
                    "Failed to append message to stream file: {}",
                    &stream.log_path
                );
                return Err(SystemError::CannotAppendMessage);
            }
            stream.current_position += size as u64;
            if file.close().await.is_err() {
                error!("Failed to close stream file: {}", &stream.log_path);
            }

            info!(
                "Appended message to stream file: {} at offset: {}, position: {}",
                &stream.log_path, stream.current_offset, stream.current_position
            );
        }

        Ok(())
    }

    pub fn poll_messages(
        &self,
        stream_id: u64,
        offset: u64,
        count: u64,
    ) -> Result<&[Message], SystemError> {
        let stream = self.streams.get(&stream_id);
        if stream.is_none() {
            return Err(SystemError::InvalidStreamId);
        }

        let stream = stream.unwrap();
        if stream.messages.is_empty() {
            return Ok(EMPTY_MESSAGES);
        }

        if offset > stream.current_offset {
            return Err(SystemError::InvalidOffset);
        }

        if count == 0 {
            return Err(SystemError::InvalidCount);
        }

        let start_offset = offset;
        let mut end_offset = offset + count - 1;
        if end_offset > stream.current_offset {
            end_offset = stream.current_offset;
        }

        let messages = stream.messages[start_offset as usize..=end_offset as usize].as_ref();
        Ok(messages)
    }
}

impl Display for Streamer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Streamer {{ path: {}, streams: {} }}",
            self.path,
            self.streams.len()
        )
    }
}

impl Display for Stream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stream {{ id: {}, path: {}, messages: {} }}",
            self.id,
            self.log_path,
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
        let stream_id = 1;
        let mut streamer = Streamer::new(&test.streams_path());
        streamer.create_stream(stream_id).await;
        let append_messages = AppendMessages {
            stream_id,
            messages: vec![
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
            ],
        };
        let result = streamer.append_messages(&append_messages).await;
        assert!(result.is_ok());
        let stream = streamer.streams.get(&stream_id).unwrap();
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
