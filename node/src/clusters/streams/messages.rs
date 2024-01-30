use crate::clusters::cluster::Cluster;
use crate::configs::config::RequiredAcknowledgements;
use crate::connection::handler::ConnectionHandler;
use crate::types::Term;
use sdk::commands::append_messages::AppendableMessage;
use sdk::error::SystemError;
use sdk::models::message::Message;
use tracing::{error, info, warn};

impl Cluster {
    pub async fn append_messages(
        &self,
        term: Term,
        stream_id: u64,
        messages: &[AppendableMessage],
    ) -> Result<(Vec<Message>, u64), SystemError> {
        let current_term = self.election_manager.get_current_term().await;
        if current_term != term {
            error!(
                "Failed to append messages to stream with ID: {stream_id}, term: {term} is not equal to current term: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(term));
        }

        let mut streamer = self.streamer.lock().await;
        streamer.append_messages(stream_id, messages).await
    }

    pub async fn commit_messages(
        &self,
        term: Term,
        stream_id: u64,
        messages: Vec<Message>,
    ) -> Result<(), SystemError> {
        let current_term = self.election_manager.get_current_term().await;
        if current_term != term {
            error!(
                "Failed to commit messages to stream with ID: {stream_id}, term: {term} is not equal to current term: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(term));
        }

        let mut streamer = self.streamer.lock().await;
        streamer.commit_messages(stream_id, messages).await
    }

    pub async fn reset_offset(&self, stream_id: u64, offset: u64) {
        warn!("Resetting offset for stream with ID: {stream_id} to: {offset}...",);
        let mut streamer = self.streamer.lock().await;
        streamer.reset_offset(stream_id, offset).await;
        warn!("Successfully reset offset for stream with ID: {stream_id} to: {offset}.");
    }

    pub async fn sync_appended_messages(
        &self,
        handler: &mut ConnectionHandler,
        term: Term,
        stream_id: u64,
        messages: &[Message],
    ) -> Result<(), SystemError> {
        if !self.is_leader().await {
            handler.send_empty_ok_response().await?;
            return Ok(());
        }

        let current_offset;
        let replication_factor;
        {
            let streamer = self.streamer.lock().await;
            let stream = streamer.get_stream(stream_id).unwrap();
            current_offset = stream.current_offset;
            replication_factor = stream.replication_factor as u64;
        }

        if replication_factor == 1 {
            info!("Replication factor is 1, no need to sync appended messages.");
            return Ok(());
        }

        let current_term = self.election_manager.get_current_term().await;
        if current_term != term {
            error!(
                "Failed to sync messages, term: {term} is not equal to current term: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(term));
        }

        let majority_required =
            self.required_acknowledgements == RequiredAcknowledgements::Majority;
        if !majority_required {
            handler.send_empty_ok_response().await?;
        }

        let mut synced_nodes = 1;
        let appendable_messages = messages
            .iter()
            .map(|message| AppendableMessage {
                id: message.id,
                payload: message.payload.clone(),
            })
            .collect::<Vec<AppendableMessage>>();
        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            if let Err(error) = node
                .node
                .sync_messages(
                    current_term,
                    stream_id,
                    current_offset,
                    &appendable_messages,
                )
                .await
            {
                error!(
                    "Failed to sync appended messages to cluster node with ID: {}, {error}",
                    node.node.id
                );
                continue;
            }

            synced_nodes += 1;
            if synced_nodes >= replication_factor {
                info!("Successfully synced appended messages to replication factor of {replication_factor} nodes.");
                if majority_required {
                    handler.send_empty_ok_response().await?;
                }

                return Ok(());
            }
        }

        error!(
            "Failed to sync appended messages to replication factor of {replication_factor} nodes.",
        );

        if !majority_required {
            return Ok(());
        }

        Err(SystemError::CannotSyncAppendedMessages)
    }
}
