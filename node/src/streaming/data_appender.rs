use crate::bytes_serializable::BytesSerializable;
use crate::streaming::file;
use crate::streaming::messages::Message;
use futures::AsyncReadExt;
use monoio::io::{AsyncReadRentExt, BufReader};
use std::fmt::{Display, Formatter};
use std::fs::create_dir_all;
use std::path::Path;
use std::str::from_utf8;
use tracing::{error, info};

const LOG_FILE: &str = "stream.log";

#[derive(Debug)]
pub struct DataAppender {
    path: String,
    stream_path: String,
    messages: Vec<Message>,
    current_offset: u64,
    current_position: u64,
}

impl DataAppender {
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
            "Initialized data appender, stream path: {}, messages count: {}",
            self.stream_path,
            self.messages.len()
        );
    }

    pub async fn append_message(&mut self, value: &str) {
        if !self.messages.is_empty() {
            self.current_offset += 1;
        }

        let message = Message::new(self.current_offset, value.to_string());
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
        }
        self.current_position += size as u64;
        info!(
            "Appended message to stream file: {} at offset: {}, position: {}",
            self.stream_path, self.current_offset, self.current_position
        );
    }

    pub async fn load_messages(&self) -> (Vec<Message>, u64) {
        let mut messages = Vec::new();
        let file = file::open(&self.stream_path)
            .await
            .unwrap_or_else(|_| panic!("Failed to read stream file: {}", self.stream_path));

        let mut position = 0u64;
        let reader = BufReader::new(file);
        loop {
            let offset = reader.buffer().read_u64_le().await;
            if offset.is_err() {
                break;
            }

            let payload_length = reader.buffer().read_u32_le().await;
            if payload_length.is_err() {
                error!("Failed to read payload length");
                break;
            }

            let payload_length = payload_length.unwrap();
            let mut payload = vec![0; payload_length as usize];
            if reader.buffer().read(&mut payload).await.is_err() {
                error!("Failed to read payload");
                break;
            }

            position += 12 + payload_length as u64;
            let value = from_utf8(&payload).unwrap().to_string();
            let message = Message::new(offset.unwrap(), value);
            messages.push(message);
        }

        (messages, position)
    }
}

impl Display for DataAppender {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataAppender {{ path: {} }}", self.path)
    }
}
