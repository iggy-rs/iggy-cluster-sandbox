use crate::clusters::cluster::Cluster;
use crate::connection::tcp_connection::TcpConnection;
use sdk::commands::create_stream::CreateStream;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut TcpConnection,
    command: &CreateStream,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    let mut streamer = cluster.streamer.lock().await;
    streamer.create_stream(command.id).await;
    handler.send_empty_ok_response().await?;
    Ok(())
}
