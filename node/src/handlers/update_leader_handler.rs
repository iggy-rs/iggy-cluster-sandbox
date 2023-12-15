use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::update_leader::UpdateLeader;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &UpdateLeader,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    cluster
        .election_manager
        .set_leader(command.term, handler.node_id)
        .await?;
    handler.send_empty_ok_response().await?;
    Ok(())
}
