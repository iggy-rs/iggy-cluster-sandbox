use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::sync_messages::SyncMessages;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::{error, info};

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &SyncMessages,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    info!(
        "Received sync messages for stream with ID: {}",
        command.stream_id
    );
    let (uncommited_messages, current_offset) = cluster
        .append_messages(command.term, command.stream_id, &command.messages)
        .await?;
    if cluster
        .commit_messages(command.term, command.stream_id, uncommited_messages)
        .await
        .is_err()
    {
        cluster
            .reset_offset(command.stream_id, current_offset)
            .await;
        error!("Failed to commit messages.")
    }
    handler.send_empty_ok_response().await?;
    info!("Sent a sync messages response.");
    Ok(())
}
