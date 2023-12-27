use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::sync_created_stream::SyncCreatedStream;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &SyncCreatedStream,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    info!(
        "Received sync created stream with ID: {}",
        command.stream_id
    );
    cluster
        .create_stream(command.term, command.stream_id)
        .await?;
    handler.send_empty_ok_response().await?;
    Ok(())
}
