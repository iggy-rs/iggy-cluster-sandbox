use crate::clusters::elections::election::ElectionManager;
use crate::clusters::nodes::node::{Node, Resiliency};
use crate::configs::config::ClusterConfig;
use crate::streaming::streamer::Streamer;
use crate::types::NodeId;
use futures::lock::Mutex;
use sdk::error::SystemError;
use sdk::models::metadata::{Metadata, NodeInfo, StreamInfo};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct SelfNode {
    pub id: u64,
    pub name: String,
    pub internal_address: String,
    pub public_address: String,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ClusterState {
    Uninitialized,
    Healthy,
}

#[derive(Debug)]
pub struct Cluster {
    pub nodes: HashMap<u64, Rc<ClusterNode>>,
    pub streamer: Mutex<Streamer>,
    pub secret: String,
    pub election_manager: ElectionManager,
    pub heartbeat_interval: Duration,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ClusterNodeState {
    Leader,
    Candidate,
    Follower,
}

#[derive(Debug)]
pub(crate) struct ClusterNode {
    pub state: Mutex<ClusterNodeState>,
    pub node: Node,
}

impl ClusterNode {
    pub async fn set_state(&self, state: ClusterNodeState) {
        *self.state.lock().await = state;
    }

    pub async fn is_leader(&self) -> bool {
        *self.state.lock().await == ClusterNodeState::Leader
    }
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

impl Cluster {
    pub fn new(
        self_node: SelfNode,
        config: &ClusterConfig,
        streamer: Streamer,
    ) -> Result<Self, SystemError> {
        let mut nodes = HashMap::new();
        let self_node_id = self_node.id;
        nodes.insert(
            self_node.id,
            Rc::new(ClusterNode {
                state: Mutex::new(ClusterNodeState::Leader),
                node: Self::create_node(
                    self_node.id,
                    &config.secret,
                    &self_node.name,
                    &self_node.internal_address,
                    &self_node.public_address,
                    self_node.clone(),
                    config,
                )?,
            }),
        );

        for node in &config.nodes {
            let cluster_node = ClusterNode {
                state: Mutex::new(ClusterNodeState::Candidate),
                node: Self::create_node(
                    node.id,
                    &config.secret,
                    &node.name,
                    &node.internal_address,
                    &node.public_address,
                    self_node.clone(),
                    config,
                )?,
            };
            nodes.insert(node.id, Rc::new(cluster_node));
        }

        Ok(Self {
            heartbeat_interval: Duration::from_millis(config.heartbeat_interval),
            election_manager: ElectionManager::new(
                self_node_id,
                nodes.len() as u64,
                (
                    config.election_timeout_range_from,
                    config.election_timeout_range_to,
                ),
            ),
            nodes,
            streamer: Mutex::new(streamer),
            secret: config.secret.to_string(),
        })
    }

    fn create_node(
        id: u64,
        secret: &str,
        node_name: &str,
        node_address: &str,
        public_address: &str,
        self_node: SelfNode,
        config: &ClusterConfig,
    ) -> Result<Node, SystemError> {
        Node::new(
            id,
            secret,
            node_name,
            node_address,
            public_address,
            self_node,
            Resiliency {
                heartbeat_interval: config.heartbeat_interval,
                reconnection_retries: config.reconnection_retries,
                reconnection_interval: config.reconnection_interval,
            },
        )
    }

    pub async fn connect(&self) -> Result<(), SystemError> {
        info!("Connecting all cluster nodes...");
        let mut connections = 0;
        let expected_connections = (self.nodes.len() / 2) + 1;
        for node in self.nodes.values() {
            if Self::init_node_connection(node.clone()).await.is_ok() {
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

        info!("All cluster nodes connected.");
        Ok(())
    }

    async fn init_node_connection(cluster_node: Rc<ClusterNode>) -> Result<(), SystemError> {
        let name = cluster_node.node.name.clone();
        if let Err(error) = Self::connect_to_node(cluster_node).await {
            error!("Failed to connect to cluster node: {name}, error: {error}",);
            return Err(error);
        }

        Ok(())
    }

    pub async fn connect_to(&self, node_id: u64) -> Result<(), SystemError> {
        let cluster_node = self.nodes.get(&node_id);
        if cluster_node.is_none() {
            return Err(SystemError::InvalidNode(node_id));
        }

        Self::connect_to_node(cluster_node.unwrap().clone()).await
    }

    pub fn get_self_node(&self) -> Option<Rc<ClusterNode>> {
        self.nodes
            .values()
            .find(|cluster_node| cluster_node.node.is_self_node())
            .cloned()
    }

    pub async fn handle_disconnected_node(&self, node_id: NodeId) {
        if node_id == 0 {
            return;
        }

        info!("Handling disconnected node ID: {}...", node_id);
        let cluster_node = self.nodes.get(&node_id);
        if cluster_node.is_none() {
            error!("Invalid node ID: {node_id}");
            return;
        }

        let cluster_node = cluster_node.unwrap();
        if cluster_node.node.disconnect().await.is_err() {
            error!("Failed to disconnect node ID: {node_id}");
        }
        let leader_id = self.election_manager.get_leader_id().await;
        if let Some(leader_id) = leader_id {
            if leader_id == node_id {
                info!("Leader node ID: {node_id} has disconnected.");
                self.election_manager.remove_leader().await;
                if let Err(error) = self.start_election().await {
                    error!("Failed to start election, error: {error}");
                }
            }
        }
        info!("Handled disconnected node ID: {node_id}.");
    }

    async fn connect_to_node(cluster_node: Rc<ClusterNode>) -> Result<(), SystemError> {
        info!(
            "Connecting to cluster node: {}, ID: {}...",
            cluster_node.node.name, cluster_node.node.id
        );
        if cluster_node.node.connect().await.is_err() {
            cluster_node.node.disconnect().await?;
            error!(
                "Failed to connect to cluster node: {}, ID: {}",
                cluster_node.node.name, cluster_node.node.id
            );
            return Err(SystemError::CannotConnectToClusterNode(
                cluster_node.node.address.to_string(),
            ));
        }
        cluster_node.node.set_connected().await;
        info!(
            "Connected to cluster node: {}, ID: {}",
            cluster_node.node.name, cluster_node.node.id
        );
        Self::start_heartbeat_for_node(cluster_node)?;
        Ok(())
    }

    fn start_heartbeat_for_node(cluster_node: Rc<ClusterNode>) -> Result<(), SystemError> {
        let node_id = cluster_node.node.id;
        let node_name = cluster_node.node.name.clone();
        info!("Starting heartbeat for node: {node_name}, ID: {node_id}...");
        monoio::spawn(async move {
            if cluster_node.node.start_heartbeat().await.is_err() {
                cluster_node
                    .node
                    .disconnect()
                    .await
                    .unwrap_or_else(|error| {
                        error!(
                            "Failed to disconnect from cluster node ID: {node_id}, error: {error}"
                        );
                    });
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
        for cluster_node in self.nodes.values() {
            cluster_node.node.disconnect().await?;
        }
        info!("All cluster nodes disconnected.");
        Ok(())
    }

    pub async fn is_connected_to(&self, node_id: u64) -> bool {
        let node = self.nodes.get(&node_id);
        if node.is_none() {
            return false;
        }

        node.unwrap().node.is_connected().await
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

    pub async fn verify_is_leader(&self) -> Result<(), SystemError> {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        let self_node = self_node.unwrap();
        if self_node.is_leader().await {
            return Ok(());
        }

        error!("This node is not a leader.");
        Err(SystemError::NotLeader)
    }

    pub async fn get_metadata(&self) -> Metadata {
        let mut metadata = Metadata {
            leader_id: self.election_manager.get_leader_id().await,
            nodes: HashMap::new(),
            streams: HashMap::new(),
        };
        metadata.nodes = self
            .nodes
            .values()
            .map(|node| {
                (
                    node.node.id,
                    NodeInfo {
                        id: node.node.id,
                        name: node.node.name.clone(),
                        address: node.node.public_address.clone(),
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
        let mut available_nodes = 1;
        let required_nodes = (self.nodes.len() / 2) + 1;
        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            if node.node.is_connected().await {
                available_nodes += 1;
            }
        }

        if available_nodes < required_nodes {
            return ClusterState::Uninitialized;
        }

        ClusterState::Healthy
    }
}
