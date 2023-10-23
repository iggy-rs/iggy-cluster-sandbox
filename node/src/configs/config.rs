use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct SystemConfig {
    pub node: NodeConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NodeConfig {
    pub address: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:8100".to_string(),
        }
    }
}
