use crate::clusters::node::Node;
use crate::configs::config::ClusterConfig;
use crate::error::SystemError;
use futures::lock::Mutex;
use std::rc::Rc;
use tracing::{error, info};

#[derive(Debug, PartialEq)]
pub enum ClusterState {
    Uninitialized,
    Healthy,
}

#[derive(Debug)]
pub struct Cluster {
    pub state: Mutex<ClusterState>,
    pub nodes: Vec<Rc<ClusterNode>>,
}

#[derive(Debug)]
pub struct ClusterNode {
    pub state: Mutex<ClusterNodeState>,
    pub node: Node,
}

#[derive(Debug, PartialEq)]
pub enum ClusterNodeState {
    Connected,
    Disconnected,
}

impl Cluster {
    pub fn new(
        self_name: &str,
        self_address: &str,
        config: &ClusterConfig,
    ) -> Result<Self, SystemError> {
        let mut nodes = Vec::new();
        nodes.push(Rc::new(ClusterNode {
            state: Mutex::new(ClusterNodeState::Connected),
            node: Self::create_node(self_name, self_address, true, config)?,
        }));
        for node in &config.nodes {
            let cluster_node = ClusterNode {
                state: Mutex::new(ClusterNodeState::Disconnected),
                node: Self::create_node(&node.name, &node.address, false, config)?,
            };
            nodes.push(Rc::new(cluster_node));
        }

        Ok(Self {
            state: Mutex::new(ClusterState::Uninitialized),
            nodes,
        })
    }

    fn create_node(
        node_name: &str,
        node_address: &str,
        is_self: bool,
        config: &ClusterConfig,
    ) -> Result<Node, SystemError> {
        Node::new(
            node_name,
            node_address,
            is_self,
            config.healthcheck_interval,
            config.reconnection_interval,
            config.reconnection_retries,
        )
    }

    pub async fn connect(&self) -> Result<(), SystemError> {
        info!("Connecting all cluster nodes...");
        for cluster_node in &self.nodes {
            let cluster_node = cluster_node.clone();
            monoio::spawn(async move {
                if cluster_node.node.connect().await.is_err() {
                    error!(
                        "Failed to connect to cluster node: {}",
                        cluster_node.node.name
                    );
                }

                *cluster_node.state.lock().await = ClusterNodeState::Connected;
            });
        }

        self.set_state(ClusterState::Healthy).await;
        info!("All cluster nodes connected.");
        Ok(())
    }

    pub fn start_healthcheck(&self) -> Result<(), SystemError> {
        info!(
            "Starting healthcheck for all cluster nodes {}...",
            self.nodes.len()
        );
        for cluster_node in &self.nodes {
            let cluster_node = cluster_node.clone();
            monoio::spawn(async move {
                if cluster_node.node.start_healthcheck().await.is_err() {
                    *cluster_node.state.lock().await = ClusterNodeState::Disconnected;
                    error!(
                        "Failed to start healthcheck for cluster node: {}",
                        cluster_node.node.name
                    );
                }
            });
        }
        info!("Healthcheck for all cluster nodes started.");
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        info!("Disconnecting all cluster nodes...");
        for cluster_node in &self.nodes {
            cluster_node.node.disconnect().await?;
            *cluster_node.state.lock().await = ClusterNodeState::Disconnected;
        }
        self.set_state(ClusterState::Uninitialized).await;
        info!("All cluster nodes disconnected.");
        Ok(())
    }

    async fn set_state(&self, state: ClusterState) {
        *self.state.lock().await = state;
    }
}
