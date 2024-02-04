use crate::clusters::cluster::Cluster;
use sdk::error::SystemError;
use tracing::error;

impl Cluster {
    pub async fn sync_nodes_state(&self) -> Result<(), SystemError> {
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

            // TODO: Implement state sync logic
            let _state = state.unwrap();
        }

        Ok(())
    }
}
