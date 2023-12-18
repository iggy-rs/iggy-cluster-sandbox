use crate::clusters::cluster::SelfNode;
use crate::clusters::node_client::{ClientState, NodeClient};
use crate::types::{NodeId, TermId};
use futures::lock::Mutex;
use monoio::time::sleep;
use sdk::error::SystemError;
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug)]
pub struct Node {
    pub id: NodeId,
    pub name: String,
    pub address: String,
    term: Mutex<TermId>,
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
        self_node: SelfNode,
        resiliency: Resiliency,
    ) -> Result<Self, SystemError> {
        let client = NodeClient::new(id, secret, self_node, address, resiliency)?;
        Ok(Self {
            id,
            name: name.to_string(),
            address: address.to_string(),
            heartbeat: NodeHeartbeat {
                interval: Duration::from_millis(resiliency.heartbeat_interval),
            },
            term: Mutex::new(0),
            leader_id: Mutex::new(None),
            client,
        })
    }

    pub async fn set_leader(&self, term: TermId, leader_id: NodeId) {
        *self.term.lock().await = term;
        *self.leader_id.lock().await = Some(leader_id);
        self.client.set_leader(term, leader_id).await;
    }

    pub fn is_self_node(&self) -> bool {
        self.client.is_self_node()
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
            let ping = self.client.ping(term, leader_id).await;
            if ping.is_ok() {
                info!("Heartbeat passed for cluster node: {}", self.name);
                continue;
            }

            error!("Heartbeat failed for cluster node: {}", self.name);
            let error = ping.unwrap_err();
            match error {
                SystemError::SendRequestFailed => {
                    error!("Failed to send a request to cluster node: {}", self.name);
                    self.connect().await?;
                    continue;
                }
                SystemError::ClientDisconnected => {
                    error!("Cluster node disconnected: {}", self.name);
                    self.connect().await?;
                    continue;
                }
                SystemError::InvalidResponse(status, _) => {
                    error!(
                        "Received invalid response with status: {status} from cluster node: {}",
                        self.name
                    );
                    self.connect().await?;
                    continue;
                }
                _ => {
                    error!(
                        "Cluster node heartbeat failed: {}. cannot recover.",
                        self.name
                    );
                    return Err(error);
                }
            }
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

        self.client.get_client_state().await == ClientState::Connected
    }
}
