use crate::clusters::cluster_node_client::ClusterNodeClient;
use crate::error::SystemError;
use monoio::time::sleep;
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug)]
pub struct Node {
    pub name: String,
    pub address: String,
    is_self: bool,
    client: ClusterNodeClient,
}

impl Node {
    pub fn new(name: &str, address: &str, is_self: bool) -> Result<Self, SystemError> {
        let client = ClusterNodeClient::new_with_defaults(address)?;
        Ok(Self {
            name: name.to_string(),
            address: address.to_string(),
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

        let interval = Duration::from_secs(3);
        info!("Starting healthcheck for cluster node: {}...", self.name);
        loop {
            sleep(interval).await;
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
