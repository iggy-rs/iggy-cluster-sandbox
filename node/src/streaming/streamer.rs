use crate::streaming::messages::Message;
use crate::streaming::stream::Stream;
use sdk::commands::append_messages::AppendableMessage;
use sdk::error::SystemError;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
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

    pub fn get_streams(&self) -> Vec<&Stream> {
        self.streams.values().collect::<Vec<&Stream>>()
    }

    pub fn set_leader(&mut self, leader_id: u64) {
        for stream in self.streams.values_mut() {
            stream.leader_id = leader_id;
        }
    }

    pub async fn create_stream(&mut self, id: u64) {
        if self.streams.contains_key(&id) {
            warn!("Stream: {id} already exists.");
            return;
        }

        let mut stream = Stream::new(id, self.node_id, &self.path);
        stream.init().await;
        self.streams.insert(id, stream);
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
        for stream in self.streams.values_mut() {
            stream.init().await;
        }
    }

    pub async fn append_messages(
        &mut self,
        stream_id: u64,
        messages: &[AppendableMessage],
    ) -> Result<(), SystemError> {
        let stream = self.streams.get_mut(&stream_id);
        if stream.is_none() {
            return Err(SystemError::InvalidStreamId);
        }

        let stream = stream.unwrap();
        stream.append_messages(messages).await
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
