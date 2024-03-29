use crate::clusters::cluster::SelfNode;
use crate::clusters::nodes::clients::node_client::NodeClient;
use crate::types::{Index, NodeId, Term};
use futures::lock::Mutex;
use monoio::time::sleep;
use sdk::commands::append_messages::AppendableMessage;
use sdk::error::SystemError;
use sdk::models::appended_state::AppendedState;
use sdk::models::log_entry::LogEntry;
use sdk::models::message::Message;
use sdk::models::node_state::NodeState;
use sdk::models::stream::Stream;
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug)]
pub struct Node {
    pub id: NodeId,
    pub name: String,
    pub address: String,
    pub public_address: String,
    pub can_be_leader: Mutex<bool>,
    initial_sync_completed: Mutex<bool>,
    term: Mutex<Term>,
    leader_id: Mutex<Option<NodeId>>,
    heartbeat: NodeHeartbeat,
    client: NodeClient,
}

#[derive(Debug, Copy, Clone)]
pub struct Resiliency {
    pub heartbeat_interval: u64,
    pub reconnection_retries: u32,
    pub reconnection_interval: u64,
}

#[derive(Debug)]
pub struct NodeHeartbeat {
    pub interval: Duration,
}

impl Node {
    pub fn new(
        id: NodeId,
        secret: &str,
        name: &str,
        address: &str,
        public_address: &str,
        self_node: SelfNode,
        resiliency: Resiliency,
    ) -> Result<Self, SystemError> {
        let client = NodeClient::new(id, secret, self_node, address, resiliency)?;
        Ok(Self {
            id,
            name: name.to_string(),
            address: address.to_string(),
            public_address: public_address.to_string(),
            heartbeat: NodeHeartbeat {
                interval: Duration::from_millis(resiliency.heartbeat_interval),
            },
            term: Mutex::new(0),
            leader_id: Mutex::new(None),
            can_be_leader: Mutex::new(true),
            initial_sync_completed: Mutex::new(false),
            client,
        })
    }

    pub async fn set_leader(&self, term: Term, leader_id: NodeId) {
        *self.term.lock().await = term;
        *self.leader_id.lock().await = Some(leader_id);
        self.client.set_leader(term, leader_id).await;
    }

    pub fn is_self_node(&self) -> bool {
        self.client.is_self_node()
    }

    pub async fn set_connected(&self) {
        self.client.set_connected().await;
    }

    pub async fn connect(&self) -> Result<(), SystemError> {
        if self.is_self_node() {
            return Ok(());
        }

        self.client.connect().await
    }

    pub async fn start_heartbeat(&self) -> Result<(), SystemError> {
        if self.is_self_node() {
            return Ok(());
        }

        info!("Starting heartbeat for cluster node: {}...", self.name);
        loop {
            sleep(self.heartbeat.interval).await;
            let term = *self.term.lock().await;
            let leader_id = *self.leader_id.lock().await;
            let heartbeat = self.client.heartbeat(term, leader_id).await;
            if heartbeat.is_ok() {
                info!("Heartbeat passed for cluster node: {}", self.name);
                continue;
            }

            let error = heartbeat.unwrap_err();
            error!("Heartbeat failed for cluster node: {}, {error}", self.name);
            return Err(error);
        }
    }

    pub async fn request_vote(&self, term: u64) -> Result<(), SystemError> {
        if self.is_self_node() {
            return Ok(());
        }

        self.client.request_vote(term).await
    }

    pub async fn update_leader(&self, term: u64, leader_id: u64) -> Result<(), SystemError> {
        if self.is_self_node() {
            return Ok(());
        }

        self.client.update_leader(term, leader_id).await
    }

    pub async fn get_streams(&self) -> Result<Vec<Stream>, SystemError> {
        if self.is_self_node() {
            return Ok(Vec::new());
        }

        self.client.get_streams().await
    }

    pub async fn load_state(&self, start_index: Index) -> Result<AppendedState, SystemError> {
        if self.is_self_node() {
            return Ok(AppendedState::default());
        }

        self.client.load_state(start_index).await
    }

    pub async fn get_node_state(&self) -> Result<NodeState, SystemError> {
        if self.is_self_node() {
            return Ok(NodeState::default());
        }

        self.client.get_node_state().await
    }

    pub async fn append_entry(
        &self,
        term: Term,
        leader_commit: Index,
        prev_log_index: Index,
        entries: Vec<LogEntry>,
    ) -> Result<(), SystemError> {
        if self.is_self_node() {
            return Ok(());
        }

        self.client
            .append_entries(term, leader_commit, prev_log_index, entries)
            .await
    }

    pub async fn sync_messages(
        &self,
        term: u64,
        stream_id: u64,
        current_offset: u64,
        messages: &[AppendableMessage],
    ) -> Result<(), SystemError> {
        if self.is_self_node() {
            return Ok(());
        }

        self.client
            .sync_messages(term, stream_id, current_offset, messages)
            .await
    }

    pub async fn poll_messages(
        &self,
        stream_id: u64,
        offset: u64,
        count: u64,
    ) -> Result<Vec<Message>, SystemError> {
        if self.is_self_node() {
            return Ok(Vec::new());
        }

        self.client.poll_messages(stream_id, offset, count).await
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        if self.is_self_node() {
            return Ok(());
        }

        self.client.disconnect().await
    }

    pub async fn is_connected(&self) -> bool {
        if self.is_self_node() {
            return true;
        }

        self.client.is_connected().await
    }

    pub async fn can_be_leader(&self) -> bool {
        *self.can_be_leader.lock().await
    }

    pub async fn set_can_be_leader(&self, can_be_leader: bool) {
        *self.can_be_leader.lock().await = can_be_leader;
    }

    pub async fn initial_sync_completed(&self) -> bool {
        *self.initial_sync_completed.lock().await
    }

    pub async fn complete_initial_sync(&self) {
        *self.initial_sync_completed.lock().await = true;
    }
}
