use crate::clusters::node::Node;
use crate::configs::config::ClusterConfig;
use crate::error::SystemError;
use std::rc::Rc;
use tracing::{error, info};

#[derive(Debug)]
pub struct Cluster {
    pub nodes: Vec<Rc<Node>>,
}

impl Cluster {
    pub fn new(
        self_name: &str,
        self_address: &str,
        config: &ClusterConfig,
    ) -> Result<Self, SystemError> {
        let mut nodes = Vec::new();
        nodes.push(Self::create_node(self_name, self_address, true, config)?);
        for node in &config.nodes {
            nodes.push(Self::create_node(&node.name, &node.address, false, config)?);
        }

        Ok(Self { nodes })
    }

    fn create_node(
        node_name: &str,
        node_address: &str,
        is_self: bool,
        config: &ClusterConfig,
    ) -> Result<Rc<Node>, SystemError> {
        Ok(Rc::new(Node::new(
            node_name,
            node_address,
            is_self,
            config.healthcheck_interval,
            config.reconnection_interval,
            config.reconnection_retries,
        )?))
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
        info!(
            "Starting healthcheck for all cluster nodes {}...",
            self.nodes.len()
        );
        for node in &self.nodes {
            let node = node.clone();
            monoio::spawn(async move {
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
