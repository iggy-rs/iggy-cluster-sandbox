use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

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
    pub name: String,
    pub address: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ClusterConfig {
    pub max_timeout: u32,
    pub healthcheck_interval: u64,
    pub reconnection_interval: u64,
    pub reconnection_retries: u32,
    pub nodes: Vec<ClusterNodeConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ClusterNodeConfig {
    pub name: String,
    pub address: String,
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
            name: "node".to_string(),
            address: "0.0.0.0:8201".to_string(),
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            max_timeout: 1000,
            healthcheck_interval: 3000,
            reconnection_interval: 1000,
            reconnection_retries: 10,
            nodes: vec![],
        }
    }
}

impl Display for NodeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ name: {}, address: {} }}", self.name, self.address)
    }
}

impl Display for ClusterConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ max_timeout: {}, members: {} }}",
            self.max_timeout,
            self.nodes
                .iter()
                .map(|m| m.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl Display for ClusterNodeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ name: {}, address: {} }}", self.name, self.address)
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
