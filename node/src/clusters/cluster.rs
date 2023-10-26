use crate::clusters::node::Node;
use crate::error::SystemError;
use tracing::info;

#[derive(Debug)]
pub struct Cluster {
    pub nodes: Vec<Node>,
}

impl Cluster {
    pub fn new(self_name: &str, self_address: &str) -> Result<Self, SystemError> {
        let nodes = vec![Node::new(self_name, self_address, true)?];
        Ok(Self { nodes })
    }

    pub fn add_node(&mut self, name: &str, address: &str) -> Result<(), SystemError> {
        let node = Node::new(name, address, false)?;
        self.nodes.push(node);
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

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        info!("Disconnecting all cluster nodes...");
        for node in &self.nodes {
            node.disconnect().await?;
        }
        info!("All cluster nodes disconnected.");
        Ok(())
    }
}
