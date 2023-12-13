use crate::clusters::election::{ElectionManager, ElectionState};
use crate::clusters::node::{Node, Resiliency};
use crate::configs::config::ClusterConfig;
use crate::streaming::streamer::Streamer;
use futures::lock::Mutex;
use sdk::error::SystemError;
use sdk::models::metadata::{Metadata, NodeInfo, StreamInfo};
use std::collections::HashMap;
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
    pub secret: String,
    pub election_manager: ElectionManager,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ClusterNodeState {
    Leader,
    Candidate,
    Follower,
}

#[derive(Debug)]
pub(crate) struct ClusterNode {
    pub connection_status: Mutex<ClusterNodeConnectionStatus>,
    pub state: Mutex<ClusterNodeState>,
    pub node: Node,
}

impl ClusterNode {
    pub async fn set_state(&self, state: ClusterNodeState) {
        *self.state.lock().await = state;
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum ClusterNodeConnectionStatus {
    Connected,
    Disconnected,
}

impl Display for ClusterNodeState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterNodeState::Leader => write!(f, "leader"),
            ClusterNodeState::Follower => write!(f, "follower"),
            ClusterNodeState::Candidate => write!(f, "candidate"),
        }
    }
}

impl Display for ClusterNodeConnectionStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterNodeConnectionStatus::Connected => write!(f, "connected"),
            ClusterNodeConnectionStatus::Disconnected => write!(f, "disconnected"),
        }
    }
}

impl Cluster {
    pub fn new(
        self_id: u64,
        self_name: &str,
        self_address: &str,
        config: &ClusterConfig,
        streamer: Streamer,
    ) -> Result<Self, SystemError> {
        let mut nodes = Vec::new();
        nodes.push(Rc::new(ClusterNode {
            connection_status: Mutex::new(ClusterNodeConnectionStatus::Connected),
            state: Mutex::new(ClusterNodeState::Leader),
            node: Self::create_node(
                self_id,
                &config.secret,
                self_name,
                self_name,
                self_address,
                true,
                config,
            )?,
        }));
        for node in &config.nodes {
            let cluster_node = ClusterNode {
                state: Mutex::new(ClusterNodeState::Follower),
                connection_status: Mutex::new(ClusterNodeConnectionStatus::Disconnected),
                node: Self::create_node(
                    node.id,
                    &config.secret,
                    &node.name,
                    self_name,
                    &node.address,
                    false,
                    config,
                )?,
            };
            nodes.push(Rc::new(cluster_node));
        }

        Ok(Self {
            state: Mutex::new(ClusterState::Uninitialized),
            nodes,
            streamer: Mutex::new(streamer),
            secret: config.secret.to_string(),
            election_manager: ElectionManager::new(self_id),
        })
    }

    fn create_node(
        id: u64,
        secret: &str,
        node_name: &str,
        self_name: &str,
        node_address: &str,
        is_self: bool,
        config: &ClusterConfig,
    ) -> Result<Node, SystemError> {
        Node::new(
            id,
            secret,
            node_name,
            self_name,
            node_address,
            is_self,
            Resiliency {
                heartbeat_interval: config.heartbeat_interval,
                reconnection_retries: config.reconnection_retries,
                reconnection_interval: config.reconnection_interval,
            },
        )
    }

    pub async fn start_election(&self) -> Result<(), SystemError> {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        let self_node = self_node.unwrap();
        self_node.set_state(ClusterNodeState::Candidate).await;
        let term = self.election_manager.next_term();
        let election_state = self.election_manager.start_election(term).await;
        match election_state {
            ElectionState::InProgress => {
                info!("Election for term: {term} is in progress...");
            }
            ElectionState::LeaderElected(leader_id) => {
                info!(
                    "Election for term: {term} has completed, leader ID: {}.",
                    leader_id
                );
                if leader_id == self_node.node.id {
                    self_node.set_state(ClusterNodeState::Leader).await;
                    info!("Your role is leader, term: {term}.");
                } else {
                    self_node.set_state(ClusterNodeState::Follower).await;
                    info!("Your role is follower, term: {term}.");
                }
            }
            ElectionState::NoLeaderElected => {
                info!("Election for term: {term} has completed, no leader elected.");
                // TODO: Request votes from other nodes.
            }
        }

        Ok(())
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

    pub async fn connect_to(&self, node_id: u64) -> Result<(), SystemError> {
        let cluster_node = self
            .nodes
            .iter()
            .find(|cluster_node| cluster_node.node.id == node_id);
        if cluster_node.is_none() {
            return Err(SystemError::InvalidNode(node_id));
        }

        Self::connect_to_node(cluster_node.unwrap()).await
    }

    fn get_self_node(&self) -> Option<Rc<ClusterNode>> {
        self.nodes
            .iter()
            .find(|cluster_node| cluster_node.node.is_self)
            .cloned()
    }

    async fn connect_to_node(cluster_node: &ClusterNode) -> Result<(), SystemError> {
        info!(
            "Connecting to cluster node: {}, ID: {}...",
            cluster_node.node.name, cluster_node.node.id
        );
        if cluster_node.node.connect().await.is_err() {
            *cluster_node.connection_status.lock().await =
                ClusterNodeConnectionStatus::Disconnected;
            error!(
                "Failed to connect to cluster node: {}, ID: {}",
                cluster_node.node.name, cluster_node.node.id
            );
            return Err(SystemError::CannotConnectToClusterNode(
                cluster_node.node.address.to_string(),
            ));
        }
        *cluster_node.connection_status.lock().await = ClusterNodeConnectionStatus::Connected;
        info!(
            "Connected to cluster node: {}, ID: {}",
            cluster_node.node.name, cluster_node.node.id
        );
        Ok(())
    }

    pub fn start_heartbeat(&self) -> Result<(), SystemError> {
        info!(
            "Starting heartbeat for all cluster nodes {}...",
            self.nodes.len()
        );
        for cluster_node in &self.nodes {
            let node_name = cluster_node.node.name.clone();
            let cluster_node = cluster_node.clone();
            Self::start_heartbeat_for_node(cluster_node).unwrap_or_else(|error| {
                error!(
                    "Failed to start heartbeat for cluster node: {node_name}, error: {}",
                    error
                );
            });
        }
        info!("Heartbeat for all cluster nodes started.");
        Ok(())
    }

    pub fn start_heartbeat_for(&self, node_id: u64) -> Result<(), SystemError> {
        info!("Starting heartbeat for node ID: {node_id}...");
        let cluster_node = self
            .nodes
            .iter()
            .find(|cluster_node| cluster_node.node.id == node_id);
        if cluster_node.is_none() {
            return Err(SystemError::InvalidNode(node_id));
        }

        Self::start_heartbeat_for_node(cluster_node.unwrap().clone())
    }

    fn start_heartbeat_for_node(cluster_node: Rc<ClusterNode>) -> Result<(), SystemError> {
        let node_id = cluster_node.node.id;
        let node_name = cluster_node.node.name.clone();
        info!("Starting heartbeat for node: {node_name}, ID: {node_id}, ...");
        monoio::spawn(async move {
            if cluster_node.node.start_heartbeat().await.is_err() {
                *cluster_node.connection_status.lock().await =
                    ClusterNodeConnectionStatus::Disconnected;
                error!(
                    "Failed to start heartbeat for cluster node: {}, ID: {node_id}.",
                    cluster_node.node.name
                );
            }
        });

        info!("Heartbeat for node: {node_name}, ID: {node_id}, started.");
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        info!("Disconnecting all cluster nodes...");
        for cluster_node in &self.nodes {
            cluster_node.node.disconnect().await?;
            *cluster_node.connection_status.lock().await =
                ClusterNodeConnectionStatus::Disconnected;
        }
        self.set_state(ClusterState::Uninitialized).await;
        info!("All cluster nodes disconnected.");
        Ok(())
    }

    pub async fn is_connected_to(&self, node_id: u64) -> bool {
        for cluster_node in &self.nodes {
            if cluster_node.node.id == node_id {
                return cluster_node.node.is_connected().await;
            }
        }

        false
    }

    pub fn validate_secret(&self, secret: &str) -> bool {
        self.secret == secret
    }

    pub async fn verify_is_healthy(&self) -> Result<(), SystemError> {
        if self.get_state().await != ClusterState::Healthy {
            return Err(SystemError::UnhealthyCluster);
        }

        Ok(())
    }

    pub async fn get_metadata(&self) -> Metadata {
        let mut metadata = Metadata {
            nodes: HashMap::new(),
            streams: HashMap::new(),
        };
        metadata.nodes = self
            .nodes
            .iter()
            .map(|node| {
                (
                    node.node.id,
                    NodeInfo {
                        id: node.node.id,
                        name: node.node.name.clone(),
                        address: node.node.address.clone(),
                    },
                )
            })
            .collect();

        metadata.streams = self
            .streamer
            .lock()
            .await
            .get_streams()
            .iter()
            .map(|stream| {
                (
                    stream.stream_id,
                    StreamInfo {
                        stream_id: stream.stream_id,
                        leader_id: stream.leader_id,
                    },
                )
            })
            .collect();
        metadata
    }

    pub async fn get_state(&self) -> ClusterState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClusterState) {
        *self.state.lock().await = state;
    }
}
