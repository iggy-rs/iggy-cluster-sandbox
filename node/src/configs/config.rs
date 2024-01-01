use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq)]
pub enum RequiredAcknowledgements {
    None,
    Leader,
    Majority,
}

impl Display for RequiredAcknowledgements {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RequiredAcknowledgements::None => write!(f, "none"),
            RequiredAcknowledgements::Leader => write!(f, "leader"),
            RequiredAcknowledgements::Majority => write!(f, "majority"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub(crate) struct SystemConfig {
    pub node: NodeConfig,
    pub cluster: ClusterConfig,
    pub stream: StreamConfig,
    pub server: ServerConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct StreamConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ServerConfig {
    pub address: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct NodeConfig {
    pub id: u64,
    pub name: String,
    pub address: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ClusterConfig {
    pub max_timeout: u32,
    pub heartbeat_interval: u64,
    pub reconnection_interval: u64,
    pub reconnection_retries: u32,
    pub secret: String,
    pub nodes: Vec<ClusterNodeConfig>,
    pub election_timeout_range_from: u64,
    pub election_timeout_range_to: u64,
    pub required_acknowledgements: RequiredAcknowledgements,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ClusterNodeConfig {
    pub id: u64,
    pub name: String,
    pub public_address: String,
    pub internal_address: String,
}

impl FromStr for RequiredAcknowledgements {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(RequiredAcknowledgements::None),
            "leader" => Ok(RequiredAcknowledgements::Leader),
            "majority" => Ok(RequiredAcknowledgements::Majority),
            _ => Err(format!("Invalid required acknowledgements value: {}", s)),
        }
    }
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            path: "local_data/streams".to_string(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:8101".to_string(),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: 1,
            name: "node".to_string(),
            address: "0.0.0.0:8201".to_string(),
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            max_timeout: 1000,
            heartbeat_interval: 3000,
            reconnection_interval: 1000,
            reconnection_retries: 10,
            secret: "secret123!".to_string(),
            nodes: vec![],
            election_timeout_range_from: 100,
            election_timeout_range_to: 300,
            required_acknowledgements: RequiredAcknowledgements::Majority,
        }
    }
}

impl Display for NodeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ id: {}, name: {}, address: {} }}",
            self.id, self.name, self.address
        )
    }
}

impl Display for ClusterConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ max_timeout: {}, heartbeat_interval: {}, reconnection_interval: {}, reconnection_retries: {}, secret: {}, nodes: {:?}, election_timeout_range_from: {}, election_timeout_range_to: {}, required_acknowledgements: {} }}",
            self.max_timeout, self.heartbeat_interval, self.reconnection_interval, self.reconnection_retries, self.secret, self.nodes, self.election_timeout_range_from, self.election_timeout_range_to, self.required_acknowledgements
        )
    }
}

impl Display for ClusterNodeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ name: {}, address: {} }}",
            self.name, self.internal_address
        )
    }
}

impl Display for SystemConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Config -> {{ node: {}, cluster: {} }}",
            self.node, self.cluster
        )
    }
}
