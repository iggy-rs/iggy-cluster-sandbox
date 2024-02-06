use crate::clusters::cluster::Cluster;
use sdk::error::SystemError;
use std::collections::HashMap;
use tracing::{error, info};

impl Cluster {
    pub async fn sync_nodes_state(&self) -> Result<(), SystemError> {
        let mut states = HashMap::new();
        let self_node = self.get_self_node();
        if self_node.is_none() {
            error!("Failed to get self node.");
            return Err(SystemError::UnhealthyCluster);
        }

        info!("Syncing state from cluster nodes...");
        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            let state = node.node.get_node_state().await;
            if state.is_err() {
                let error = state.unwrap_err();
                error!(
                    "Failed to sync state from cluster node with ID: {}, {error}",
                    node.node.id
                );
                continue;
            }

            let state = state.unwrap();
            states.insert(node.node.id, state);
        }
        info!("Synced state from cluster nodes.");

        // TODO: Validate other nodes states and check if this node can be a leader.
        let self_node = self_node.unwrap();
        self_node.node.set_can_be_leader(true).await;

        Ok(())
    }
}
