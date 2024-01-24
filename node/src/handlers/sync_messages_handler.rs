use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::sync_messages::SyncMessages;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

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
    let uncommited_messages = cluster
        .append_messages(command.term, command.stream_id, &command.messages)
        .await?;
    cluster
        .commit_messages(command.term, command.stream_id, uncommited_messages)
        .await?;
    cluster.set_high_watermark(command.offset).await?;
    handler.send_empty_ok_response().await?;
    info!("Sent a sync messages response.");
    Ok(())
}
