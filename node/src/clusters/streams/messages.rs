use crate::clusters::cluster::Cluster;
use crate::types::Term;
use sdk::commands::append_messages::AppendableMessage;
use sdk::error::SystemError;
use tracing::{error, info};

impl Cluster {
    pub async fn append_messages(
        &self,
        term: Term,
        stream_id: u64,
        messages: &[AppendableMessage],
    ) -> Result<(), SystemError> {
        let current_term = self.election_manager.get_current_term().await;
        if current_term != term {
            error!(
                "Failed to append messages, term: {term} is not equal to current term: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(term));
        }

        let mut streamer = self.streamer.lock().await;
        streamer.append_messages(stream_id, messages).await?;
        if !self.is_leader().await {
            return Ok(());
        }

        let mut synced_nodes = 1;
        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            if let Err(error) = node
                .node
                .sync_messages(current_term, stream_id, messages)
                .await
            {
                error!(
                    "Failed to sync created stream to cluster node with ID: {}, {error}",
                    node.node.id
                );
                continue;
            }

            synced_nodes += 1;
        }

        let quorum = self.get_quorum_count();
        if synced_nodes >= quorum {
            info!("Successfully synced appended messages to quorum of nodes.");
            return Ok(());
        }

        error!(
            "Failed to sync appended messages to quorum of nodes, synced nodes: {synced_nodes} < quorum: {quorum}..",
        );

        // TODO: Write-ahead log to revert appended messages.

        Ok(())
    }
}
