use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::ping::Ping;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::warn;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &Ping,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.election_manager.set_term(command.term).await;
    if let Some(leader_id) = command.leader_id {
        if cluster.set_leader(command.term, leader_id).await.is_err() {
            warn!(
                "Failed to set the leader ID: {} for term: {}.",
                leader_id, command.term
            );
        }
    }

    handler.send_empty_ok_response().await?;
    Ok(())
}
