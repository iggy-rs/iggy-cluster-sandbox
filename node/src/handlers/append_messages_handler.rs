use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::append_messages::AppendMessages;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &AppendMessages,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    info!("Received an append messages command");
    let mut streamer = cluster.streamer.lock().await;
    streamer.append_messages(command).await?;
    handler.send_empty_ok_response().await?;
    info!("Sent an append messages response.");
    Ok(())
}
