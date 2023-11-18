use crate::clusters::node::Node;
use crate::configs::config::ClusterConfig;
use crate::error::SystemError;
use crate::streaming::data_appender::DataAppender;
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
    pub data_appender: Mutex<DataAppender>,
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
        data_appender: DataAppender,
    ) -> Result<Self, SystemError> {
        let mut nodes = Vec::new();
        nodes.push(Rc::new(ClusterNode {
            state: Mutex::new(ClusterNodeState::Connected),
            node: Self::create_node(self_name, self_name, self_address, true, config)?,
        }));
        for node in &config.nodes {
            let cluster_node = ClusterNode {
                state: Mutex::new(ClusterNodeState::Disconnected),
                node: Self::create_node(&node.name, self_name, &node.address, false, config)?,
            };
            nodes.push(Rc::new(cluster_node));
        }

        Ok(Self {
            state: Mutex::new(ClusterState::Uninitialized),
            nodes,
            data_appender: Mutex::new(data_appender),
        })
    }

    fn create_node(
        node_name: &str,
        self_name: &str,
        node_address: &str,
        is_self: bool,
        config: &ClusterConfig,
    ) -> Result<Node, SystemError> {
        Node::new(
            node_name,
            self_name,
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
                Self::connect_to_node(&cluster_node)
                    .await
                    .unwrap_or_else(|error| {
                        error!(
                            "Failed to connect to cluster node: {}, error: {}",
                            cluster_node.node.name, error
                        );
                    });
            });
        }
        self.set_state(ClusterState::Healthy).await;
        info!("All cluster nodes connected.");
        Ok(())
    }

    pub async fn connect_to(&self, node_name: &str) -> Result<(), SystemError> {
        let cluster_node = self
            .nodes
            .iter()
            .find(|cluster_node| cluster_node.node.name == node_name);
        if cluster_node.is_none() {
            return Err(SystemError::InvalidNode(node_name.to_string()));
        }

        Self::connect_to_node(cluster_node.unwrap()).await
    }

    async fn connect_to_node(cluster_node: &ClusterNode) -> Result<(), SystemError> {
        info!("Connecting to cluster node: {}", cluster_node.node.name);
        if cluster_node.node.connect().await.is_err() {
            *cluster_node.state.lock().await = ClusterNodeState::Disconnected;
            error!(
                "Failed to connect to cluster node: {}",
                cluster_node.node.name
            );
            return Err(SystemError::CannotConnectToClusterNode(
                cluster_node.node.address.to_string(),
            ));
        }
        *cluster_node.state.lock().await = ClusterNodeState::Connected;
        info!("Connected to cluster node: {}", cluster_node.node.name);
        Ok(())
    }

    pub fn start_healthcheck(&self) -> Result<(), SystemError> {
        info!(
            "Starting healthcheck for all cluster nodes {}...",
            self.nodes.len()
        );
        for cluster_node in &self.nodes {
            let node_name = cluster_node.node.name.clone();
            let cluster_node = cluster_node.clone();
            Self::start_healthcheck_for_node(cluster_node).unwrap_or_else(|error| {
                error!(
                    "Failed to start healthcheck for cluster node: {node_name}, error: {}",
                    error
                );
            });
        }
        info!("Healthcheck for all cluster nodes started.");
        Ok(())
    }

    pub fn start_healthcheck_for(&self, node_name: &str) -> Result<(), SystemError> {
        info!("Starting healthcheck for node: {node_name}...");
        let cluster_node = self
            .nodes
            .iter()
            .find(|cluster_node| cluster_node.node.name == node_name);
        if cluster_node.is_none() {
            return Err(SystemError::InvalidNode(node_name.to_string()));
        }

        Self::start_healthcheck_for_node(cluster_node.unwrap().clone())
    }

    fn start_healthcheck_for_node(cluster_node: Rc<ClusterNode>) -> Result<(), SystemError> {
        let node_name = cluster_node.node.name.clone();
        info!("Starting healthcheck for node: {node_name}...");
        monoio::spawn(async move {
            if cluster_node.node.start_healthcheck().await.is_err() {
                *cluster_node.state.lock().await = ClusterNodeState::Disconnected;
                error!(
                    "Failed to start healthcheck for cluster node: {}",
                    cluster_node.node.name
                );
            }
        });

        info!("Healthcheck for node: {node_name} started.");
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

    pub async fn is_connected_to(&self, node_name: &str) -> bool {
        for cluster_node in &self.nodes {
            if cluster_node.node.name == node_name {
                return cluster_node.node.is_connected().await;
            }
        }

        false
    }

    async fn set_state(&self, state: ClusterState) {
        *self.state.lock().await = state;
    }
}
