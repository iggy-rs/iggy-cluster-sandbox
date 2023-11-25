use crate::clusters::cluster::{Cluster, ClusterState};
use crate::connection::tcp_handler::TcpHandler;
use sdk::commands::sync_messages::SyncMessages;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::{info, warn};

pub async fn handle(
    handler: &mut TcpHandler,
    _: &SyncMessages,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    if cluster.get_state().await != ClusterState::Healthy {
        warn!("Cluster is not healthy, unable to append messages.");
        handler
            .send_error_response(SystemError::UnhealthyCluster)
            .await?;
        return Ok(());
    }

    info!("Received send messages command");
    handler.send_empty_ok_response().await?;
    info!("Sent a sync messages response.");
    Ok(())
}
