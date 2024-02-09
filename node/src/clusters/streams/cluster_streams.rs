use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use crate::types::{NodeId, Term};
use sdk::commands::create_stream::CreateStream;
use sdk::commands::delete_stream::DeleteStream;
use sdk::error::SystemError;
use tracing::{error, info};

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

    pub async fn sync_streams_from_other_nodes(
        &self,
        available_leaders: &[NodeId],
    ) -> Result<(), SystemError> {
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
                    "Failed to sync streams from cluster node with ID: {}, {error}",
                    node.node.id
                );
                continue;
            }

            let streams = streams.unwrap();
            for stream in streams {
                info!("Syncing stream: {stream} from cluster node with ID: {node_id}");
                let mut streamer = self.streamer.lock().await;
                streamer.create_stream(stream.id, 3).await?;
                let self_stream = streamer.get_stream(stream.id).unwrap();
                if self_stream.high_watermark == stream.high_watermark {
                    info!(
                        "Stream: {stream} is already in sync with cluster node with ID: {node_id}"
                    );
                    continue;
                }

                // TODO: Sync messages based on high watermark
            }
        }

        Ok(())
    }
}
