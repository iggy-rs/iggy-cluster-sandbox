use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::append_messages::AppendMessages;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::error;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &AppendMessages,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    cluster.verify_is_leader().await?;
    let term = cluster.election_manager.get_current_term().await;
    let uncommited_messages = cluster
        .append_messages(term, command.stream_id, &command.messages)
        .await?;
    cluster
        .sync_appended_messages(handler, term, command.stream_id, &uncommited_messages)
        .await?;
    if cluster
        .commit_messages(term, command.stream_id, uncommited_messages)
        .await
        .is_err()
    {
        error!("Failed to commit messages.")
    }
    Ok(())
}
