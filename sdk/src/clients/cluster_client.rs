use crate::bytes_serializable::BytesSerializable;
use crate::clients::node_client::NodeClient;
use crate::commands::append_messages::{AppendMessages, AppendableMessage};
use crate::commands::command::Command;
use crate::commands::create_stream::CreateStream;
use crate::commands::delete_stream::DeleteStream;
use crate::commands::get_metadata::GetMetadata;
use crate::commands::get_streams::GetStreams;
use crate::commands::ping::Ping;
use crate::commands::poll_messages::PollMessages;
use crate::error::SystemError;
use crate::models::message::Message;
use crate::models::metadata::Metadata;
use crate::models::stream::Stream;
use bytes::Bytes;
use futures::lock::Mutex;
use monoio::time::sleep;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use tracing::{error, info, warn};

#[derive(Debug)]
pub struct ClusterClient {
    clients: HashMap<String, Rc<Mutex<Option<NodeClient>>>>,
    metadata: Mutex<Option<Metadata>>,
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
            metadata: Mutex::new(None),
            clients: addresses
                .iter()
                .map(|address| (address.to_string(), Rc::new(Mutex::new(None))))
                .collect(),
        }
    }

    pub async fn init(&mut self) -> Result<(), SystemError> {
        let addresses = self.clients.keys().cloned().collect::<Vec<String>>();
        info!(
            "Connecting to Iggy cluster, nodes: {}",
            addresses.join(", ")
        );

        let mut retry_count = 0;
        let mut connected_clients = HashMap::new();
        for (address, client) in &self.clients {
            let client = client.lock().await;
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
                .insert(address, Rc::new(Mutex::new(Some(client))));
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
        let leader_address = self.get_leader_address().await?;
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

    pub async fn get_streams(&self) -> Result<Vec<Stream>, SystemError> {
        let leader_address = self.get_leader_address().await?;
        let command = GetStreams::new_command();
        let bytes = self.send(&command, &leader_address).await?;
        let mut streams = Vec::new();
        let mut position = 0;
        while position < bytes.len() {
            let stream = Stream::from_bytes(&bytes[position..position + 16])?;
            position += 16;
            streams.push(stream);
        }
        Ok(streams)
    }

    pub async fn ping(&self) -> Result<(), SystemError> {
        let command = Ping::new_command();
        let address = self.get_first_available_node_address().await?;
        self.send(&command, &address).await?;
        Ok(())
    }

    pub async fn create_stream(
        &self,
        stream_id: u64,
        replication_factor: Option<u8>,
    ) -> Result<(), SystemError> {
        let leader_address = self.get_leader_address().await?;
        let command = CreateStream::new_command(stream_id, replication_factor);
        self.send(&command, &leader_address).await?;
        Ok(())
    }

    pub async fn delete_stream(&self, stream_id: u64) -> Result<(), SystemError> {
        let leader_address = self.get_leader_address().await?;
        let command = DeleteStream::new_command(stream_id);
        self.send(&command, &leader_address).await?;
        Ok(())
    }

    pub async fn append_messages(
        &self,
        stream_id: u64,
        messages: Vec<AppendableMessage>,
    ) -> Result<(), SystemError> {
        let leader_address = self.get_leader_address().await?;
        let command = AppendMessages::new_command(stream_id, messages);
        self.send(&command, &leader_address).await?;
        Ok(())
    }

    pub async fn update_metadata(&self) -> Result<(), SystemError> {
        let address = self.get_first_available_node_address().await?;
        let command = GetMetadata::new_command();
        let bytes = self.send(&command, &address).await?;
        let metadata = Metadata::from_bytes(&bytes)?;
        info!("Updated the metadata: {metadata}");
        self.metadata.lock().await.replace(metadata);
        Ok(())
    }

    async fn get_first_available_node_address(&self) -> Result<String, SystemError> {
        for (address, client) in &self.clients {
            let client = client.lock().await;
            if let Some(client) = client.as_ref() {
                if client.is_connected().await {
                    return Ok(address.to_string());
                }
            }
        }

        Err(SystemError::UnhealthyCluster)
    }

    async fn get_leader_address(&self) -> Result<String, SystemError> {
        let metadata = self.metadata.lock().await;
        if metadata.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        let metadata = metadata.as_ref().unwrap();
        if metadata.leader_id.is_none() {
            return Err(SystemError::LeaderNotElected);
        }

        let leader_id = metadata.leader_id.unwrap();
        let node = metadata.nodes.get(&leader_id);
        if node.is_none() {
            return Err(SystemError::InvalidNode(leader_id));
        }

        let node = node.unwrap();
        let address = &node.address;
        let node_client = self.clients.get(address);
        if node_client.is_none() {
            error!("Leader not found for address: {address}");
            return Err(SystemError::InvalidClusterNodeAddress(address.to_string()));
        }

        let node_client = node_client.unwrap().lock().await;
        if node_client.is_none() {
            error!("Leader not found for address: {address}");
            return Err(SystemError::InvalidClusterNodeAddress(address.to_string()));
        }

        let node_client = node_client.as_ref().unwrap();
        if !node_client.is_connected().await {
            error!("Leader not found for address: {address}");
            return Err(SystemError::LeaderDisconnected);
        }

        Ok(address.to_string())
    }

    async fn send(&self, command: &Command, address: &str) -> Result<Vec<u8>, SystemError> {
        let client = self.clients.get(address);
        if client.is_none() {
            warn!("No client for address: {address}");
            return Err(SystemError::InvalidClusterNodeAddress(address.to_string()));
        }

        let mut client = client.unwrap().lock().await;
        if client.is_none() {
            info!("Connecting to Iggy node at address: {address}...");
            let node_client = NodeClient::init(address).await?;
            client.replace(node_client);
        }

        let node_client = client.as_mut().unwrap();
        if !node_client.is_connected().await {
            info!("Reconnecting to Iggy node at address: {address}...");
            node_client.connect().await?;
        }

        let result = node_client.send(command).await;
        if result.is_ok() {
            return result;
        }

        let error = result.err().unwrap();
        if let SystemError::CannotReadResponse = error {
            info!("Reconnecting to Iggy node at address: {address}...");
            let node_client = NodeClient::init(address).await?;
            client.replace(node_client);
            let node_client = client.as_mut().unwrap();
            return node_client.send(command).await;
        }

        Err(error)
    }
}
