use crate::clusters::cluster::Cluster;
use crate::connection::tcp_connection::TcpConnection;
use sdk::commands::sync_messages::SyncMessages;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut TcpConnection,
    _: &SyncMessages,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    info!("Received send messages command");
    handler.send_empty_ok_response().await?;
    info!("Sent a sync messages response.");
    Ok(())
}
