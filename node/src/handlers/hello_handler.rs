use crate::clusters::cluster::Cluster;
use crate::connection::tcp_connection::TcpConnection;
use sdk::commands::hello::Hello;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut TcpConnection,
    command: &Hello,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    info!(
        "Received a hello command, secret: {}, name: {}, ID: {}.",
        command.secret, command.name, command.id
    );
    if !cluster.validate_secret(&command.secret) {
        info!("Invalid cluster secret: {}.", command.secret);
        handler
            .send_error_response(SystemError::InvalidClusterSecret)
            .await?;
        return Err(SystemError::InvalidClusterSecret);
    }

    info!("Received a valid cluster secret.");
    handler.send_empty_ok_response().await?;
    info!("Sent a hello response.");
    if cluster.is_connected_to(command.id).await {
        info!(
            "The node: {}, ID: {} is already connected.",
            command.name, command.id
        );
        return Ok(());
    }

    info!(
        "Connecting to the disconnected node: {}, ID: {}...",
        command.name, command.id
    );
    cluster.connect_to(command.id).await?;
    cluster.start_heartbeat_for(command.id)?;
    info!(
        "Connected to the previously disconnected node: {}, ID: {}.",
        command.name, command.id
    );
    Ok(())
}
