use crate::clusters::cluster::Cluster;
use crate::configs::config::RequiredAcknowledgements;
use crate::connection::handler::ConnectionHandler;
use crate::types::Term;
use bytes::Bytes;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use sdk::models::log_entry::LogEntry;
use tracing::{error, info};

impl Cluster {
    pub async fn sync_state(
        &self,
        handler: &mut ConnectionHandler,
        term: Term,
        command: Command,
    ) -> Result<(), SystemError> {
        if !self.is_leader().await {
            handler.send_empty_ok_response().await?;
            return Ok(());
        }

        let current_term = self.election_manager.get_current_term().await;
        if current_term != term {
            error!(
                "Failed to sync state, term: {term} is not equal to current term: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(term));
        }

        let leader_commit;
        let prev_log_index;
        let log_entry;
        {
            let bytes = Bytes::from(command.as_bytes());
            (leader_commit, prev_log_index, log_entry) = self.append_state(bytes).await?;
        }

        let majority_required =
            self.required_acknowledgements == RequiredAcknowledgements::Majority;
        if !majority_required {
            handler.send_empty_ok_response().await?;
        }

        let mut synced_nodes = 1;
        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            let entries = vec![LogEntry {
                index: log_entry.index,
                data: log_entry.data.clone(),
            }];
            if let Err(error) = node
                .node
                .append_entry(current_term, leader_commit, prev_log_index, entries)
                .await
            {
                error!(
                    "Failed to sync state to cluster node with ID: {}, {error}",
                    node.node.id
                );
                continue;
            }

            synced_nodes += 1;
        }

        let quorum = self.get_quorum_count();
        if synced_nodes >= quorum {
            info!("Successfully synced state to quorum of nodes.");
            if majority_required {
                handler.send_empty_ok_response().await?;
            }

            return Ok(());
        }

        error!(
            "Failed to sync state to quorum of nodes, synced nodes: {synced_nodes} < quorum: {quorum}",
        );

        if !majority_required {
            return Ok(());
        }

        Err(SystemError::CannotSyncState)
    }
}
