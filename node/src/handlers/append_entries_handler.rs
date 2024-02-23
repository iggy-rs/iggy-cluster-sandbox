use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::append_entries::AppendEntries;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &AppendEntries,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    info!("Received append entries command.",);
    cluster
        .can_sync_state(command.leader_commit, command.prev_log_index)
        .await?;
    cluster.replay_state(command.term, &command.entries).await?;
    handler.send_empty_ok_response().await?;
    info!("Sent an append entries response.");
    Ok(())
}
