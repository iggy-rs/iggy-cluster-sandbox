use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use crate::types::{NodeId, Term};
use sdk::commands::create_stream::CreateStream;
use sdk::commands::delete_stream::DeleteStream;
use sdk::error::SystemError;
use std::cmp::Ordering;
use tracing::{error, info, warn};

impl Cluster {
    pub async fn create_stream(
        &self,
        term: Term,
        stream_id: u64,
        replication_factor: Option<u8>,
    ) -> Result<(), SystemError> {
        let current_term = self.election_manager.get_current_term().await;
        if current_term != term {
            error!(
                "Failed to create stream, term: {term} is not equal to current term: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(term));
        }

        let nodes_count = self.nodes.len() as u8;
        let replication_factor = replication_factor.unwrap_or(3);
        if replication_factor > nodes_count {
            error!(
                "Failed to create stream, replication factor: {replication_factor} is greater than number of nodes: {nodes_count}.",
            );
            return Err(SystemError::InvalidReplicationFactor(replication_factor));
        }

        self.streamer
            .lock()
            .await
            .create_stream(stream_id, replication_factor)
            .await
    }

    pub async fn delete_stream(&self, term: Term, stream_id: u64) -> Result<(), SystemError> {
        let current_term = self.election_manager.get_current_term().await;
        if current_term != term {
            error!(
                "Failed to delete stream, term: {term} is not equal to current term: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(term));
        }

        self.streamer.lock().await.delete_stream(stream_id).await;
        Ok(())
    }

    pub async fn sync_created_stream(
        &self,
        handler: &mut ConnectionHandler,
        term: Term,
        stream_id: u64,
        replication_factor: Option<u8>,
    ) -> Result<(), SystemError> {
        info!("Syncing created stream with ID: {stream_id} to quorum of nodes.");
        if let Err(error) = self
            .sync_state(
                handler,
                term,
                CreateStream::new_command(stream_id, replication_factor),
            )
            .await
        {
            error!("Failed to sync created stream with ID: {stream_id}, {error}",);
            self.streamer.lock().await.delete_stream(stream_id).await;
            return Err(SystemError::CannotSyncCreatedStream);
        }
        info!("Successfully synced created stream with ID: {stream_id} to quorum of nodes.");
        Ok(())
    }

    pub async fn sync_deleted_stream(
        &self,
        handler: &mut ConnectionHandler,
        term: Term,
        stream_id: u64,
    ) -> Result<(), SystemError> {
        info!("Syncing deleted stream with ID: {stream_id} to quorum of nodes.");
        if let Err(error) = self
            .sync_state(handler, term, DeleteStream::new_command(stream_id))
            .await
        {
            error!("Failed to sync deleted stream with ID: {stream_id}, {error}",);
            return Err(SystemError::CannotSyncDeletedStream);
        }
        info!("Successfully synced deleted stream with ID: {stream_id} to quorum of nodes.");
        Ok(())
    }

    pub async fn sync_state_and_streams_from_other_nodes(
        &self,
        available_leaders: &[NodeId],
    ) -> Result<(), SystemError> {
        let mut completed = true;
        let self_state = self.get_node_state().await?;
        let last_applied = self_state.last_applied;
        let start_index = last_applied + 1;
        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            let node_id = node.node.id;
            if !available_leaders.contains(&node_id) {
                continue;
            }

            let streams = node.node.get_streams().await;
            if streams.is_err() {
                let error = streams.unwrap_err();
                error!(
                    "Failed to get streams from cluster node with ID: {}, {error}",
                    node.node.id
                );
                completed = false;
                continue;
            }

            let node_state = node.node.get_node_state().await;
            if node_state.is_err() {
                let error = node_state.unwrap_err();
                error!(
                    "Failed to get node state from cluster node with ID: {}, {error}",
                    node.node.id
                );
                completed = false;
                continue;
            }

            let node_state = node_state.unwrap();
            match last_applied.cmp(&node_state.last_applied) {
                Ordering::Less => {
                    warn!(
                        "This node is ahead of cluster node with ID: {node_id} and will be truncated."
                    );
                    completed = false;
                    let state = self.state.lock().await;
                    state.truncate(start_index).await?;
                }
                Ordering::Equal => {
                    info!("This node and cluster node with ID: {node_id} are in sync.");
                }
                Ordering::Greater => {
                    let loaded_state = node.node.load_state(start_index).await;
                    if loaded_state.is_err() {
                        let error = loaded_state.unwrap_err();
                        error!(
                            "Failed to load state from cluster node with ID: {}, {error}",
                            node.node.id
                        );
                        completed = false;
                        continue;
                    }

                    let loaded_state = loaded_state.unwrap();
                    for entry in loaded_state.entries {
                        self.append_entry(&entry).await?;
                    }
                }
            }

            let streams = streams.unwrap();
            if streams.is_empty() {
                completed = true;
                continue;
            }

            for stream in streams {
                info!("Syncing stream: {stream} from cluster node with ID: {node_id}");
                let mut streamer = self.streamer.lock().await;
                streamer.create_stream(stream.id, 3).await?;
                let self_stream = streamer.get_stream_mut(stream.id).unwrap();
                if self_stream.high_watermark == stream.high_watermark {
                    info!(
                        "Stream: {stream} is already in sync with cluster node with ID: {node_id}"
                    );
                    completed = true;
                    continue;
                }

                if self_stream.high_watermark > stream.high_watermark {
                    info!(
                        "Stream: {stream} is ahead of cluster node with ID: {node_id} and will be truncated."
                    );
                    self_stream.truncate(stream.high_watermark).await?;
                }

                let offset = self_stream.high_watermark + 1;
                let count = stream.high_watermark - self_stream.high_watermark;
                info!(
                    "Polling messages for stream: {stream} from cluster node with ID: {node_id}, offset: {offset}, count: {count}..."
                );
                let messages = node.node.poll_messages(stream.id, offset, count).await;
                if messages.is_err() {
                    let error = messages.unwrap_err();
                    error!(
                        "Failed to poll messages for stream: {stream} from cluster node with ID: {node_id}, {error}",
                    );
                    completed = false;
                    continue;
                }
                let messages = messages.unwrap();
                info!(
                    "Successfully polled {} messages for stream: {stream} from cluster node with ID: {node_id}", messages.len()
                );
                self_stream.commit_messages(messages).await?;
                self_stream.set_offset(stream.high_watermark);
                self_stream.set_high_watermark(stream.high_watermark).await;
                completed = true;
            }
        }

        if !completed {
            return Err(SystemError::CannotSyncStreams);
        }

        let self_node = self.get_self_node().unwrap();
        self_node.node.complete_initial_sync().await;
        info!("Successfully synced streams from other nodes.");
        Ok(())
    }
}
