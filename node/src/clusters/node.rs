use crate::clusters::node_client::{ClientState, NodeClient};
use crate::types::NodeId;
use monoio::time::sleep;
use sdk::error::SystemError;
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug)]
pub struct Node {
    pub id: NodeId,
    pub name: String,
    pub address: String,
    heartbeat: NodeHeartbeat,
    pub is_self: bool,
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
        self_name: &str,
        address: &str,
        is_self: bool,
        resiliency: Resiliency,
    ) -> Result<Self, SystemError> {
        let client = NodeClient::new(id, secret, self_name, address, resiliency)?;
        Ok(Self {
            id,
            name: name.to_string(),
            address: address.to_string(),
            heartbeat: NodeHeartbeat {
                interval: Duration::from_millis(resiliency.heartbeat_interval),
            },
            is_self,
            client,
        })
    }

    pub async fn connect(&self) -> Result<(), SystemError> {
        if self.is_self {
            return Ok(());
        }

        self.client.connect().await
    }

    pub async fn start_heartbeat(&self) -> Result<(), SystemError> {
        if self.is_self {
            return Ok(());
        }

        info!("Starting heartbeat for cluster node: {}...", self.name);
        loop {
            sleep(self.heartbeat.interval).await;
            let ping = self.client.ping().await;
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
                SystemError::InvalidResponse => {
                    error!("Received invalid response from cluster node: {}", self.name);
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
        if self.is_self {
            return Ok(());
        }

        self.client.request_vote(term).await
    }

    pub async fn update_leader(&self, term: u64) -> Result<(), SystemError> {
        if self.is_self {
            return Ok(());
        }

        self.client.update_leader(term).await
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        if self.is_self {
            return Ok(());
        }

        self.client.disconnect().await
    }

    pub async fn is_connected(&self) -> bool {
        if self.is_self {
            return true;
        }

        self.client.get_client_state().await == ClientState::Connected
    }
}
