use crate::clusters::cluster::Cluster;
use crate::connection::tcp_handler::TcpHandler;
use sdk::commands::hello::Hello;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub async fn handle(
    handler: &mut TcpHandler,
    command: &Hello,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    info!("Received a hello command, name: {}.", command.name);
    handler.send_empty_ok_response().await?;
    info!("Sent a hello response.");
    if cluster.is_connected_to(&command.name).await {
        info!("The node: {} is already connected.", command.name);
        return Ok(());
    }

    info!("Connecting to the disconnected node: {}...", command.name);
    cluster.connect_to(&command.name).await?;
    cluster.start_healthcheck_for(&command.name)?;
    info!(
        "Connected to the previously disconnected node: {}.",
        command.name
    );
    Ok(())
}