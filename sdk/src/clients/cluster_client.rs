use crate::bytes_serializable::BytesSerializable;
use crate::clients::node_client::NodeClient;
use crate::commands::append_messages::{AppendMessages, AppendableMessage};
use crate::commands::command::Command;
use crate::commands::create_stream::CreateStream;
use crate::commands::get_metadata::GetMetadata;
use crate::commands::poll_messages::PollMessages;
use crate::error::SystemError;
use crate::models::message::Message;
use crate::models::metadata::Metadata;
use bytes::Bytes;
use futures::lock::Mutex;
use monoio::time::sleep;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug)]
pub struct ClusterClient {
    clients: HashMap<String, Option<Rc<Mutex<NodeClient>>>>,
    metadata: Option<Mutex<Metadata>>,
    reconnection_interval: u64,
    reconnection_retries: u32,
}

impl ClusterClient {
    pub fn new(
        addresses: Vec<&str>,
        reconnection_interval: u64,
        reconnection_retries: u32,
    ) -> Self {
        Self {
            reconnection_interval,
            reconnection_retries,
            metadata: None,
            clients: addresses
                .iter()
                .map(|address| (address.to_string(), None))
                .collect(),
        }
    }

    pub async fn init(&mut self) -> Result<(), SystemError> {
        let addresses = self.clients.keys().cloned().collect::<Vec<String>>();
        info!(
            "Connecting to Iggy cluster, nodes: {}",
            addresses.join(", ")
        );
        if self.clients.iter().all(|(_, client)| client.is_some()) {
            info!("Already connected to Iggy cluster.");
            return Ok(());
        }

        let mut retry_count = 0;
        let mut connected_clients = HashMap::new();
        for (address, client) in &self.clients {
            if client.is_some() {
                info!("Already connected to Iggy node at {address}.");
                continue;
            }

            let mut connected_client;
            loop {
                info!("Connecting to Iggy node at {address}...");
                connected_client = NodeClient::init(address).await;
                if connected_client.is_ok() {
                    break;
                }

                error!("Failed to connect to Iggy node at: {address}");
                if retry_count < self.reconnection_retries {
                    retry_count += 1;
                    info!(
                        "Retrying ({}/{}) to connect to Iggy node: {} in: {} ms...",
                        retry_count, self.reconnection_retries, address, self.reconnection_interval
                    );
                    sleep(Duration::from_millis(self.reconnection_interval)).await;
                    continue;
                }

                return Err(SystemError::CannotConnectToClusterNode(address.clone()));
            }

            connected_clients.insert(address.clone(), connected_client.unwrap());
            info!("Connected to Iggy node at {address}.");
        }

        for (address, client) in connected_clients {
            self.clients
                .insert(address, Some(Rc::new(Mutex::new(client))));
        }

        info!("Connected to Iggy cluster, updating the metadata...");
        self.update_metadata().await?;
        info!("Updated the metadata.");
        Ok(())
    }

    pub async fn poll_messages(
        &self,
        stream_id: u64,
        offset: u64,
        count: u64,
    ) -> Result<Vec<Message>, SystemError> {
        let leader_address = self.get_leader_address(stream_id).await?;
        let command = PollMessages::new_command(stream_id, offset, count);
        let bytes = self.send(&command, &leader_address).await?;
        let mut messages = Vec::new();
        let mut position = 0;
        while position < bytes.len() {
            let offset = u64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
            let id = u64::from_le_bytes(bytes[position + 8..position + 16].try_into().unwrap());
            let payload_length =
                u32::from_le_bytes(bytes[position + 16..position + 20].try_into().unwrap());
            let payload =
                Bytes::from(bytes[position + 20..position + 20 + payload_length as usize].to_vec());
            position += 20 + payload_length as usize;
            let message = Message {
                offset,
                id,
                payload,
            };
            messages.push(message);
        }
        Ok(messages)
    }

    pub async fn create_stream(&self, stream_id: u64) -> Result<(), SystemError> {
        let command = CreateStream::new_command(stream_id);
        let address = self.get_first_node_address()?;
        self.send(&command, &address).await?;
        Ok(())
    }

    pub async fn append_messages(
        &self,
        stream_id: u64,
        messages: Vec<AppendableMessage>,
    ) -> Result<(), SystemError> {
        let leader_address = self.get_leader_address(stream_id).await?;
        let command = AppendMessages::new_command(stream_id, messages);
        self.send(&command, &leader_address).await?;
        Ok(())
    }

    pub async fn get_metadata(&self) -> Result<Metadata, SystemError> {
        let address = self.get_first_node_address()?;
        let command = GetMetadata::new_command();
        let bytes = self.send(&command, &address).await?;
        let metadata = Metadata::from_bytes(&bytes)?;
        Ok(metadata)
    }

    async fn update_metadata(&mut self) -> Result<(), SystemError> {
        let metadata = self.get_metadata().await?;
        self.metadata = Some(Mutex::new(metadata));
        Ok(())
    }

    fn get_first_node_address(&self) -> Result<String, SystemError> {
        let client = self.clients.iter().find(|(_, client)| client.is_some());
        if client.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        let (address, _) = client.unwrap();
        Ok(address.to_string())
    }

    async fn get_leader_address(&self, stream_id: u64) -> Result<String, SystemError> {
        let metadata = self.metadata.as_ref().unwrap().lock().await;
        let stream = metadata.streams.get(&stream_id);
        if stream.is_none() {
            return Err(SystemError::InvalidStreamId);
        }

        let leader_id = stream.unwrap().leader_id;
        let node = metadata.nodes.get(&leader_id);
        if node.is_none() {
            return Err(SystemError::InvalidNode(leader_id));
        }

        let node = node.unwrap();
        let address = &node.address;
        if !self.clients.contains_key(address) {
            return Err(SystemError::InvalidClusterNodeAddress(address.to_string()));
        }

        Ok(address.to_string())
    }

    async fn send(&self, command: &Command, address: &str) -> Result<Vec<u8>, SystemError> {
        if self.clients.iter().all(|(_, client)| client.is_none()) {
            return Err(SystemError::UnhealthyCluster);
        }

        let client = self.clients.get(address);
        if client.is_none() {
            return Err(SystemError::InvalidClusterNodeAddress(address.to_string()));
        }

        let client = client.unwrap();
        if client.is_none() {
            return Err(SystemError::CannotConnectToClusterNode(address.to_string()));
        }

        let client = client.as_ref().unwrap();
        let mut client = client.lock().await;
        client.send(command).await
    }
}
