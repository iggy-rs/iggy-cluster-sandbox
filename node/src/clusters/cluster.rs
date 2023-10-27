use crate::clusters::node::Node;
use crate::error::SystemError;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Debug)]
pub struct Cluster {
    pub nodes: Vec<Arc<Node>>,
}

impl Cluster {
    pub fn new(self_name: &str, self_address: &str) -> Result<Self, SystemError> {
        let nodes = vec![Arc::new(Node::new(self_name, self_address, true)?)];
        Ok(Self { nodes })
    }

    pub fn add_node(&mut self, name: &str, address: &str) -> Result<(), SystemError> {
        let node = Node::new(name, address, false)?;
        self.nodes.push(Arc::new(node));
        Ok(())
    }

    pub async fn connect(&self) -> Result<(), SystemError> {
        info!("Connecting all cluster nodes...");
        for node in &self.nodes {
            node.connect().await?;
        }
        info!("All cluster nodes connected.");
        Ok(())
    }

    pub fn start_healthcheck(&self) -> Result<(), SystemError> {
        info!("Starting healthcheck for all cluster nodes {}...", self.nodes.len());
        for node in &self.nodes {
            info!("About to start healthcheck for cluster node: {}", node.name);
            let node = node.clone();
            monoio::spawn(async move {
                info!("Starting healthcheck for cluster node (out): {}", node.name);
                if node.start_healthcheck().await.is_err() {
                    error!(
                        "Failed to start healthcheck for cluster node: {}",
                        node.name
                    );
                }
            });
        }
        info!("Healthcheck for all cluster nodes started.");
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        info!("Disconnecting all cluster nodes...");
        for node in &self.nodes {
            node.disconnect().await?;
        }
        info!("All cluster nodes disconnected.");
        Ok(())
    }
}
