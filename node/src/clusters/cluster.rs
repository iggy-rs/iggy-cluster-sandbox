use crate::clusters::node::Node;
use crate::configs::config::ClusterConfig;
use crate::streaming::streamer::Streamer;
use futures::lock::Mutex;
use sdk::error::SystemError;
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use tracing::{error, info};

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ClusterState {
    Uninitialized,
    Healthy,
}

#[derive(Debug)]
pub struct Cluster {
    pub state: Mutex<ClusterState>,
    pub nodes: Vec<Rc<ClusterNode>>,
    pub streamer: Mutex<Streamer>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ClusterNodeRole {
    Leader,
    Follower,
}

#[derive(Debug)]
pub(crate) struct ClusterNode {
    pub state: Mutex<ClusterNodeState>,
    pub role: ClusterNodeRole,
    pub node: Node,
}

#[derive(Debug, PartialEq)]
pub(crate) enum ClusterNodeState {
    Connected,
    Disconnected,
}

impl Display for ClusterNodeRole {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterNodeRole::Leader => write!(f, "leader"),
            ClusterNodeRole::Follower => write!(f, "follower"),
        }
    }
}

impl Display for ClusterNodeState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterNodeState::Connected => write!(f, "connected"),
            ClusterNodeState::Disconnected => write!(f, "disconnected"),
        }
    }
}

impl Cluster {
    pub fn new(
        self_name: &str,
        self_address: &str,
        config: &ClusterConfig,
        streamer: Streamer,
    ) -> Result<Self, SystemError> {
        let mut nodes = Vec::new();
        nodes.push(Rc::new(ClusterNode {
            state: Mutex::new(ClusterNodeState::Connected),
            role: ClusterNodeRole::Leader,
            node: Self::create_node(self_name, self_name, self_address, true, config)?,
        }));
        for node in &config.nodes {
            let cluster_node = ClusterNode {
                role: ClusterNodeRole::Follower,
                state: Mutex::new(ClusterNodeState::Disconnected),
                node: Self::create_node(&node.name, self_name, &node.address, false, config)?,
            };
            nodes.push(Rc::new(cluster_node));
        }

        Ok(Self {
            state: Mutex::new(ClusterState::Uninitialized),
            nodes,
            streamer: Mutex::new(streamer),
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
        let mut connections = 0;
        let expected_connections = (self.nodes.len() / 2) + 1;
        for node in self.nodes.clone() {
            if Self::init_node_connection(node).await.is_ok() {
                connections += 1;
            }
        }

        if connections < expected_connections {
            error!(
                "Not enough cluster nodes connected. Expected: {}, actual: {}",
                expected_connections, connections
            );
            return Err(SystemError::UnhealthyCluster);
        }

        self.set_state(ClusterState::Healthy).await;
        info!("All cluster nodes connected.");
        Ok(())
    }

    async fn init_node_connection(cluster_node: Rc<ClusterNode>) -> Result<(), SystemError> {
        if let Err(error) = Self::connect_to_node(&cluster_node).await {
            error!(
                "Failed to connect to cluster node: {}, error: {}",
                cluster_node.node.name, error
            );
            return Err(error);
        }

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
        info!(
            "Connecting to cluster node: {}, role: {}",
            cluster_node.node.name, cluster_node.role
        );
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

    pub async fn get_state(&self) -> ClusterState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClusterState) {
        *self.state.lock().await = state;
    }
}
