use crate::models::appended_messages::AppendedMessages;
use crate::streaming::stream::Stream;
use sdk::commands::append_messages::AppendableMessage;
use sdk::error::SystemError;
use sdk::models::message::Message;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::create_dir_all;
use std::path::Path;
use tracing::{info, warn};

#[derive(Debug)]
pub(crate) struct Streamer {
    path: String,
    node_id: u64,
    streams: HashMap<u64, Stream>,
}

impl Streamer {
    pub fn new(node_id: u64, path: &str) -> Self {
        Self {
            node_id,
            path: path.to_string(),
            streams: HashMap::new(),
        }
    }

    pub fn get_stream(&self, id: u64) -> Option<&Stream> {
        self.streams.get(&id)
    }

    pub fn get_streams(&self) -> Vec<&Stream> {
        self.streams.values().collect::<Vec<&Stream>>()
    }

    pub fn set_leader(&mut self, leader_id: u64) {
        for stream in self.streams.values_mut() {
            stream.leader_id = leader_id;
        }
    }

    pub async fn create_stream(
        &mut self,
        id: u64,
        replication_factor: u8,
    ) -> Result<(), SystemError> {
        if self.streams.contains_key(&id) {
            warn!("Stream: {id} already exists.");
            return Ok(());
        }

        let mut stream = Stream::new(id, self.node_id, &self.path, replication_factor);
        stream.init().await;
        self.streams.insert(id, stream);
        Ok(())
    }

    pub async fn delete_stream(&mut self, id: u64) {
        let stream = self.streams.remove(&id);
        if stream.is_none() {
            warn!("Stream with ID: {id} does not exist.");
            return;
        }

        let stream = stream.unwrap();
        stream.delete();
        info!("Deleted stream with ID: {id}.");
    }

    pub async fn init(&mut self) {
        if !Path::new(&self.path).exists() {
            create_dir_all(&self.path)
                .unwrap_or_else(|_| panic!("Failed to create streams directory: {}", self.path));
            info!("Created streams directory: {}", self.path);
            return;
        }

        let directory = std::fs::read_dir(&self.path)
            .unwrap_or_else(|_| panic!("Failed to read streams directory: {}", self.path));

        for entry in directory {
            let entry = entry.unwrap();
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let stream_id = path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();

            let mut stream = Stream::new(stream_id, self.node_id, &self.path, 1);
            stream.init().await;
            self.streams.insert(stream_id, stream);
            info!("Initialized stream with ID: {}", stream_id);
        }
    }

    pub async fn append_messages(
        &mut self,
        stream_id: u64,
        messages: &[AppendableMessage],
    ) -> Result<AppendedMessages, SystemError> {
        let stream = self.streams.get_mut(&stream_id);
        if stream.is_none() {
            return Err(SystemError::InvalidStreamId);
        }

        let stream = stream.unwrap();
        stream.append_messages(messages).await
    }

    pub async fn commit_messages(
        &mut self,
        stream_id: u64,
        messages: Vec<Message>,
    ) -> Result<(), SystemError> {
        let stream = self.streams.get_mut(&stream_id);
        if stream.is_none() {
            return Err(SystemError::InvalidStreamId);
        }

        let stream = stream.unwrap();
        stream.commit_messages(messages).await
    }

    pub async fn reset_offset(&mut self, stream_id: u64, offset: u64) {
        let stream = self.streams.get_mut(&stream_id);
        if stream.is_none() {
            return;
        }

        let stream = stream.unwrap();
        stream.reset_offset(offset).await
    }

    pub(crate) fn poll_messages(
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
        stream.poll_messages(offset, count)
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
