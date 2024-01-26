use crate::clusters::cluster::Cluster;
use crate::configs::config::RequiredAcknowledgements;
use crate::connection::handler::ConnectionHandler;
use crate::types::Term;
use sdk::commands::append_messages::AppendableMessage;
use sdk::error::SystemError;
use sdk::models::message::Message;
use tracing::{error, info};

impl Cluster {
    pub async fn append_messages(
        &self,
        term: Term,
        stream_id: u64,
        messages: &[AppendableMessage],
    ) -> Result<Vec<Message>, SystemError> {
        let current_term = self.election_manager.get_current_term().await;
        if current_term != term {
            error!(
                "Failed to append messages, term: {term} is not equal to current term: {current_term}.",
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
                "Failed to commit messages, term: {term} is not equal to current term: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(term));
        }

        let mut streamer = self.streamer.lock().await;
        streamer.commit_messages(stream_id, messages).await
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

        let current_offset = self
            .streamer
            .lock()
            .await
            .get_stream(stream_id)
            .unwrap()
            .current_offset;
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
        }

        let quorum = self.get_quorum_count();
        if synced_nodes >= quorum {
            info!("Successfully synced appended messages to quorum of nodes.");
            if majority_required {
                handler.send_empty_ok_response().await?;
            }

            return Ok(());
        }

        error!(
            "Failed to sync appended messages to quorum of nodes, synced nodes: {synced_nodes} < quorum: {quorum}.",
        );

        if !majority_required {
            return Ok(());
        }

        // TODO: Write-ahead log to revert appended messages.

        Err(SystemError::CannotSyncAppendedMessages)
    }
}
