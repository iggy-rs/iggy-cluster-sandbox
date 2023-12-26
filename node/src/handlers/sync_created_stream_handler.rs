use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::sync_created_stream::SyncCreatedStream;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    _: &SyncCreatedStream,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    info!("Received sync created stream command");
    handler.send_empty_ok_response().await?;
    info!("Sent a sync created stream response.");
    Ok(())
}
