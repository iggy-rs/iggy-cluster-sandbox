use crate::clusters::cluster::{Cluster, ClusterState};
use crate::connection::tcp_connection::TcpConnection;
use sdk::commands::append_messages::AppendMessages;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut TcpConnection,
    command: &AppendMessages,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    if cluster.get_state().await != ClusterState::Healthy {
        return Err(SystemError::UnhealthyCluster);
    }

    info!("Received an append messages command");
    let mut streamer = cluster.streamer.lock().await;
    streamer.append_messages(command).await?;
    handler.send_empty_ok_response().await?;
    info!("Sent an append messages response.");
    Ok(())
}
