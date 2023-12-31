use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::request_vote::RequestVote;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &RequestVote,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    let self_node = cluster.get_self_node().unwrap();
    info!(
        "Received a request vote from node ID: {}, address: {} in term: {}.",
        handler.node_id, handler.address, command.term
    );
    cluster
        .vote(command.term, handler.node_id, self_node.node.id)
        .await?;
    handler.send_empty_ok_response().await?;
    Ok(())
}
