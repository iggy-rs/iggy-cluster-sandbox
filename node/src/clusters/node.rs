use crate::clusters::cluster_node_client::ClusterNodeClient;
use crate::error::SystemError;
use tracing::info;

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

        info!("Starting healthcheck for cluster node: {}...", self.name);
        self.client.start_healthcheck().await;
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        if self.is_self {
            return Ok(());
        }

        self.client.disconnect().await
    }
}
