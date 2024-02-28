use crate::clusters::cluster::Cluster;
use crate::clusters::nodes::node::Node;
use crate::configs::config::RequiredAcknowledgements;
use crate::connection::handler::ConnectionHandler;
use crate::types::{Index, Term};
use bytes::Bytes;
use sdk::commands::command;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use sdk::models::log_entry::LogEntry;
use std::cmp::Ordering;
use tracing::{error, info, warn};

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
                size: log_entry.size,
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

    pub async fn sync_state_from_leader(
        &self,
        last_applied: Index,
        node: &Node,
    ) -> Result<(), SystemError> {
        let start_index = last_applied + 1;
        let node_id = node.id;
        let node_state = node.get_node_state().await;
        if node_state.is_err() {
            let error = node_state.unwrap_err();
            error!(
                "Failed to get node state from cluster node with ID: {}, {error}",
                node.id
            );
            return Err(error);
        }

        let node_state = node_state.unwrap();
        match last_applied.cmp(&node_state.last_applied) {
            Ordering::Less => {
                warn!(
                    "This node is ahead of cluster node with ID: {node_id} and will be truncated."
                );
                let mut state = self.state.lock().await;
                state.truncate(start_index).await?;
                return Ok(());
            }
            Ordering::Equal => {
                info!("This node and cluster node with ID: {node_id} are in sync.");
            }
            Ordering::Greater => {
                let loaded_state = node.load_state(start_index).await;
                if loaded_state.is_err() {
                    let error = loaded_state.unwrap_err();
                    error!(
                        "Failed to load state from cluster node with ID: {}, {error}",
                        node.id
                    );
                    return Err(error);
                }

                let loaded_state = loaded_state.unwrap();
                for entry in loaded_state.entries {
                    self.append_entry(&entry).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn replay_state(&self, term: Term, entries: &[LogEntry]) -> Result<(), SystemError> {
        info!(
            "Replaying state for term: {term} and {} entries.",
            entries.len()
        );
        for entry in entries {
            match command::map_from_bytes(&entry.data)? {
                Command::CreateStream(create_stream) => {
                    self.create_stream(
                        term,
                        create_stream.id,
                        create_stream.replication_factor.unwrap_or(3),
                    )
                    .await?;
                }
                Command::DeleteStream(delete_stream) => {
                    self.delete_stream(term, delete_stream.id).await?;
                }
                other => {
                    warn!("Received an unknown log entry command: {other}",);
                    return Err(SystemError::InvalidCommand);
                }
            }
        }
        Ok(())
    }
}
