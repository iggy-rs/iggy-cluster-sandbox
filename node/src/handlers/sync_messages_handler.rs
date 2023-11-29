use crate::clusters::cluster::{Cluster, ClusterState};
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
    if cluster.get_state().await != ClusterState::Healthy {
        return Err(SystemError::UnhealthyCluster);
    }

    info!("Received send messages command");
    handler.send_empty_ok_response().await?;
    info!("Sent a sync messages response.");
    Ok(())
}
