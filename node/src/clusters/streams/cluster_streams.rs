use crate::clusters::cluster::Cluster;
use crate::configs::config::RequiredAcknowledgements;
use crate::connection::handler::ConnectionHandler;
use crate::types::Term;
use bytes::Bytes;
use sdk::commands::create_stream::CreateStream;
use sdk::error::SystemError;
use sdk::models::log_entry::LogEntry;
use tracing::{error, info};

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
        Ok(())
    }

    pub async fn sync_created_stream(
        &self,
        handler: &mut ConnectionHandler,
        term: Term,
        stream_id: u64,
    ) -> Result<(), SystemError> {
        if !self.is_leader().await {
            handler.send_empty_ok_response().await?;
            return Ok(());
        }

        let current_term = self.election_manager.get_current_term().await;
        if current_term != term {
            error!(
                "Failed to sync created stream, term: {term} is not equal to current term: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(term));
        }

        let leader_commit;
        let prev_log_index;
        let log_entry;
        {
            let create_stream = CreateStream::new_command(stream_id);
            let bytes = Bytes::from(create_stream.as_bytes());
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
                    "Failed to sync created stream with ID: {stream_id} to cluster node with ID: {}, {error}",
                    node.node.id
                );
                continue;
            }

            synced_nodes += 1;
        }

        let quorum = self.get_quorum_count();
        if synced_nodes >= quorum {
            info!("Successfully synced created stream with ID: {stream_id} to quorum of nodes.");
            if majority_required {
                handler.send_empty_ok_response().await?;
            }

            return Ok(());
        }

        error!(
            "Failed to sync created stream to with ID: {stream_id} quorum of nodes, synced nodes: {synced_nodes} < quorum: {quorum}, reverting stream creation...",
        );
        self.streamer.lock().await.delete_stream(stream_id).await;

        if !majority_required {
            return Ok(());
        }

        Err(SystemError::CannotSyncCreatedStream)
    }

    pub async fn sync_streams_from_other_nodes(&self) -> Result<(), SystemError> {
        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            let streams = node.node.get_streams().await;
            if streams.is_err() {
                let error = streams.unwrap_err();
                error!(
                    "Failed to sync streams from cluster node with ID: {}, {error}",
                    node.node.id
                );
                continue;
            }

            let node_id = node.node.id;
            let streams = streams.unwrap();
            for stream in streams {
                info!("Syncing stream: {stream} from cluster node with ID: {node_id}");
                self.streamer.lock().await.create_stream(stream.id).await;
            }
        }

        Ok(())
    }
}
