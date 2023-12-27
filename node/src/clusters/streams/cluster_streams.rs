use crate::clusters::cluster::Cluster;
use crate::types::Term;
use sdk::error::SystemError;
use tracing::error;

impl Cluster {
    pub async fn create_stream(&self, term: Term, stream_id: u64) -> Result<(), SystemError> {
        let current_term = self.election_manager.get_current_term().await;
        if current_term != term {
            error!(
                "Failed to create stream, term: {term} is not equal to current term: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(term));
        }

        self.streamer.lock().await.create_stream(stream_id).await;
        if !self.is_leader().await {
            return Ok(());
        }

        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            if let Err(error) = node.node.sync_created_stream(current_term, stream_id).await {
                error!(
                    "Failed to sync created stream to cluster node with ID: {}, {error}",
                    node.node.id
                );
            }
        }

        Ok(())
    }
}
