use crate::clusters::cluster_node_client::ClusterNodeClient;
use crate::error::SystemError;
use monoio::time::sleep;
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug)]
pub struct Node {
    pub name: String,
    pub address: String,
    healthcheck: NodeHealthcheck,
    is_self: bool,
    client: ClusterNodeClient,
}

#[derive(Debug)]
pub struct NodeHealthcheck {
    pub interval: Duration,
}

impl Node {
    pub fn new(
        name: &str,
        address: &str,
        is_self: bool,
        healthcheck_interval: u64,
        reconnection_interval: u64,
        reconnection_retries: u32,
    ) -> Result<Self, SystemError> {
        let client = ClusterNodeClient::new(address, reconnection_retries, reconnection_interval)?;
        Ok(Self {
            name: name.to_string(),
            address: address.to_string(),
            healthcheck: NodeHealthcheck {
                interval: Duration::from_millis(healthcheck_interval),
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

    pub async fn start_healthcheck(&self) -> Result<(), SystemError> {
        if self.is_self {
            return Ok(());
        }

        info!("Starting healthcheck for cluster node: {}...", self.name);
        loop {
            sleep(self.healthcheck.interval).await;
            if let Err(error) = self.client.ping().await {
                error!("Healthcheck failed for cluster node: {}", self.name);
                match error {
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
                            "Cluster node healthcheck failed: {}. cannot recover.",
                            self.name
                        );
                        return Err(error);
                    }
                }
            }
        }
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        if self.is_self {
            return Ok(());
        }

        self.client.disconnect().await
    }
}
