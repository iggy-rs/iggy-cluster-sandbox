use crate::clusters::cluster::SelfNode;
use crate::clusters::nodes::node::Resiliency;
use crate::connection::handler::ConnectionHandler;
use crate::types::{Index, NodeId, Term};
use futures::lock::Mutex;
use monoio::net::TcpStream;
use monoio::time::sleep;
use sdk::bytes_serializable::BytesSerializable;
use sdk::commands::append_entries::AppendEntries;
use sdk::commands::append_messages::AppendableMessage;
use sdk::commands::get_node_state::GetNodeState;
use sdk::commands::get_streams::GetStreams;
use sdk::commands::heartbeat::Heartbeat;
use sdk::commands::hello::Hello;
use sdk::commands::request_vote::RequestVote;
use sdk::commands::sync_messages::SyncMessages;
use sdk::commands::update_leader::UpdateLeader;
use sdk::error::SystemError;
use sdk::models::log_entry::LogEntry;
use sdk::models::node_state::NodeState;
use sdk::models::stream::Stream;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

impl SelfNode {
    pub fn new(id: u64, name: &str, internal_address: &str, public_address: &str) -> Self {
        Self {
            id,
            name: name.to_string(),
            internal_address: internal_address.to_string(),
            public_address: public_address.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct NodeClient {
    pub id: u64,
    pub secret: String,
    pub self_node: SelfNode,
    pub address: SocketAddr,
    pub handler: Mutex<Option<ConnectionHandler>>,
    connected: Mutex<bool>,
    resiliency: Resiliency,
    term: Mutex<Term>,
    leader_id: Mutex<Option<NodeId>>,
}

impl NodeClient {
    pub fn new(
        id: u64,
        secret: &str,
        self_node: SelfNode,
        address: &str,
        resiliency: Resiliency,
    ) -> Result<Self, SystemError> {
        let node_address = address;
        let address = address.parse::<SocketAddr>();
        if address.is_err() {
            return Err(SystemError::InvalidClusterNodeAddress(
                node_address.to_string(),
            ));
        }

        Ok(Self {
            id,
            secret: secret.to_string(),
            self_node,
            address: address.unwrap(),
            handler: Mutex::new(None),
            connected: Mutex::new(false),
            resiliency,
            term: Mutex::new(0),
            leader_id: Mutex::new(None),
        })
    }

    pub fn is_self_node(&self) -> bool {
        self.id == self.self_node.id
    }

    pub async fn set_leader(&self, term: Term, leader_id: NodeId) {
        *self.term.lock().await = term;
        *self.leader_id.lock().await = Some(leader_id);
    }

    pub async fn connect(&self) -> Result<(), SystemError> {
        if self.is_connected().await {
            warn!(
                "Already connected to cluster node ID: {}, address: {}",
                self.id, self.address
            );
            return Ok(());
        }

        let mut retry_count = 0;
        let remote_address;
        let elapsed;
        loop {
            if self.is_connected().await {
                return Ok(());
            }

            info!(
                "Connecting to cluster node ID: {}, address: {}...",
                self.id, self.address
            );
            let now = Instant::now();
            let connection = TcpStream::connect(self.address).await;
            if connection.is_err() {
                error!(
                    "Failed to connect to cluster node ID: {}, address: {}.",
                    self.id, self.address
                );
                if retry_count < self.resiliency.reconnection_retries {
                    retry_count += 1;
                    info!(
                        "Retrying ({}/{}) to connect to cluster node ID: {}, address: {}, in: {} ms...",
                        retry_count,
                        self.resiliency.reconnection_retries,
                        self.id,
                        self.address,
                        self.resiliency.reconnection_interval
                    );
                    sleep(Duration::from_millis(self.resiliency.reconnection_interval)).await;
                    continue;
                }

                return Err(SystemError::CannotConnectToClusterNode(
                    self.address.to_string(),
                ));
            }

            elapsed = now.elapsed();
            let stream = connection.unwrap();
            remote_address = stream.peer_addr()?;
            self.handler.lock().await.replace(ConnectionHandler::new(
                stream,
                remote_address,
                self.id,
            ));
            self.set_connected().await;
            break;
        }

        info!(
            "Connected to cluster node ID: {}, address: {remote_address} in {} ms. Sending hello message...",
            self.id,
            elapsed.as_millis()
        );

        let term = *self.term.lock().await;
        let leader_id = *self.leader_id.lock().await;
        self.send_request(&Hello::new_command(
            self.secret.clone(),
            self.self_node.name.clone(),
            self.self_node.id,
            term,
            leader_id,
        ))
        .await?;
        info!(
            "Sent hello message to cluster node ID: {}, address: {}",
            self.id, self.address
        );
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        if !self.is_connected().await {
            return Ok(());
        }

        info!(
            "Disconnecting from cluster node ID: {}, address: {}...",
            self.id, self.address
        );
        self.set_disconnected().await;
        self.handler.lock().await.take();
        info!(
            "Disconnected from cluster node ID: {}, address: {}.",
            self.id, self.address
        );
        Ok(())
    }

    pub async fn heartbeat(&self, term: u64, leader_id: Option<u64>) -> Result<(), SystemError> {
        debug!(
            "Sending a heartbeat to cluster node ID: {}, address: {}...",
            self.id, self.address
        );
        let now = Instant::now();
        if let Err(error) = self
            .send_request(&Heartbeat::new_command(term, leader_id))
            .await
        {
            error!(
                "Failed to send a heartbeat to cluster node ID: {}, address: {}",
                self.id, self.address
            );
            self.set_disconnected().await;
            return Err(error);
        }
        let elapsed = now.elapsed();
        debug!(
            "Received a heartbeat from cluster node ID: {}, address: {} in {} ms.",
            self.id,
            self.address,
            elapsed.as_millis()
        );
        Ok(())
    }

    pub async fn request_vote(&self, term: u64) -> Result<(), SystemError> {
        info!(
            "Sending a request vote to cluster node ID: {}, address: {} in term: {}...",
            self.id, self.address, term
        );
        let command = RequestVote::new_command(term);
        if let Err(error) = self.send_request(&command).await {
            error!(
                "Failed to send a request vote to cluster node ID: {}, address: {} in term: {}.",
                self.id, self.address, term
            );
            return Err(error);
        }
        info!(
            "Received a request vote response from cluster node ID: {}, address: {} in term: {}.",
            self.id, self.address, term
        );
        Ok(())
    }

    pub async fn update_leader(&self, term: u64, leader_id: u64) -> Result<(), SystemError> {
        info!(
            "Sending an update leader ID: {leader_id} to cluster node ID: {}, address: {} in term: {term}...",
            self.id, self.address
        );
        let command = UpdateLeader::new_command(term, leader_id);
        self.send_request(&command).await?;
        if let Err(error) = self.send_request(&command).await {
            error!(
                "Failed to send an update leader ID: {leader_id} to cluster node ID: {}, address: {} in term: {term}.",
                self.id, self.address
            );
            return Err(error);
        }
        info!(
            "Received an update leader ID: {leader_id} response from cluster node ID: {}, address: {} in term: {term}.",
            self.id, self.address,
        );
        Ok(())
    }

    pub async fn get_streams(&self) -> Result<Vec<Stream>, SystemError> {
        info!(
            "Sending a get streams to cluster node ID: {}, address: {}...",
            self.id, self.address
        );
        let command = GetStreams::new_command();
        let result = self.send_request(&command).await;
        if result.is_err() {
            error!(
                "Failed to send a get streams to cluster node ID: {}, address: {}.",
                self.id, self.address
            );
            return Err(result.unwrap_err());
        }

        info!(
            "Received a get streams response from cluster node ID: {}, address: {}.",
            self.id, self.address
        );

        let bytes = result.unwrap();
        let mut streams = Vec::new();
        let mut position = 0;
        while position < bytes.len() {
            let stream = Stream::from_bytes(&bytes[position..position + 16])?;
            position += 16;
            streams.push(stream);
        }

        Ok(streams)
    }

    pub async fn get_node_state(&self) -> Result<NodeState, SystemError> {
        info!(
            "Sending a get node info to cluster node ID: {}, address: {}...",
            self.id, self.address
        );

        let command = GetNodeState::new_command();
        let result = self.send_request(&command).await;
        if result.is_err() {
            error!(
                "Failed to send a get node state to cluster node ID: {}, address: {}.",
                self.id, self.address
            );
            return Err(result.unwrap_err());
        }

        info!(
            "Received a get node state response from cluster node ID: {}, address: {}.",
            self.id, self.address
        );

        let node_state = NodeState::from_bytes(&result.unwrap())?;
        Ok(node_state)
    }

    pub async fn append_entries(
        &self,
        term: Term,
        leader_commit: Index,
        prev_log_index: Index,
        entries: Vec<LogEntry>,
    ) -> Result<(), SystemError> {
        info!(
            "Sending an append entry to cluster node ID: {}, address: {} in term: {}...",
            self.id, self.address, term
        );
        let leader_id = self.leader_id.lock().await.unwrap();
        let command =
            AppendEntries::new_command(term, leader_id, leader_commit, prev_log_index, entries);
        if let Err(error) = self.send_request(&command).await {
            error!(
                "Failed to send an append entry to cluster node ID: {}, address: {} in term: {}.",
                self.id, self.address, term
            );
            return Err(error);
        }
        info!(
            "Received an append entry response from cluster node ID: {}, address: {} in term: {}.",
            self.id, self.address, term
        );
        Ok(())
    }

    pub async fn sync_messages(
        &self,
        term: u64,
        stream_id: u64,
        current_offset: u64,
        messages: &[AppendableMessage],
    ) -> Result<(), SystemError> {
        info!(
            "Sending a sync messages to cluster node ID: {}, address: {} in term: {}...",
            self.id, self.address, term
        );
        let mut synced_messages = Vec::with_capacity(messages.len());
        for message in messages {
            synced_messages.push(AppendableMessage {
                id: message.id,
                payload: message.payload.clone(),
            });
        }

        let command = SyncMessages::new_command(term, stream_id, current_offset, synced_messages);
        if let Err(error) = self.send_request(&command).await {
            error!(
                "Failed to send a sync messages to cluster node ID: {}, address: {} in term: {}.",
                self.id, self.address, term
            );
            return Err(error);
        }
        info!(
            "Received a sync messages response from cluster node ID: {}, address: {} in term: {}.",
            self.id, self.address, term
        );
        Ok(())
    }

    pub async fn is_connected(&self) -> bool {
        *self.connected.lock().await
    }

    pub async fn set_connected(&self) {
        *self.connected.lock().await = true;
    }

    pub async fn set_disconnected(&self) {
        *self.connected.lock().await = false;
    }
}
