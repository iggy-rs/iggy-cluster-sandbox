use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::hello::Hello;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::{info, warn};

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &Hello,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    if !cluster.validate_secret(&command.secret) {
        warn!(
            "Invalid cluster secret: {} from node ID: {}.",
            command.secret, command.node_id
        );
        handler
            .send_error_response(SystemError::InvalidClusterSecret)
            .await?;
        return Err(SystemError::InvalidClusterSecret);
    }

    info!(
        "Received a valid cluster secret from node ID: {}.",
        command.node_id
    );
    handler.node_id = command.node_id;
    handler.send_empty_ok_response().await?;
    info!("Sent a hello response to node ID: {}.", command.node_id);

    cluster.election_manager.set_term(command.term).await;
    if let Some(leader_id) = command.leader_id {
        if cluster.set_leader(command.term, leader_id).await.is_err() {
            warn!(
                "Failed to set the leader ID: {} for term: {}.",
                leader_id, command.term
            );
        }
    }

    if cluster.is_connected_to(command.node_id).await {
        info!(
            "The node: {}, ID: {} is already connected.",
            command.name, command.node_id
        );
        return Ok(());
    }

    info!(
        "Connecting to the disconnected node: {}, ID: {}...",
        command.name, command.node_id
    );
    cluster.connect_to(command.node_id).await?;
    info!(
        "Connected to the previously disconnected node: {}, ID: {}.",
        command.name, command.node_id
    );
    Ok(())
}
