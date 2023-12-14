use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::send_vote::SendVote;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &SendVote,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    info!("Received send vote command");
    cluster
        .vote(command.term, command.candidate_id, handler.node_id)
        .await?;
    handler.send_empty_ok_response().await?;
    info!("Sent a send vote response.");
    Ok(())
}
