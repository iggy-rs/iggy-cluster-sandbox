use crate::clusters::cluster::Cluster;
use crate::connection::tcp_connection::TcpConnection;
use sdk::bytes_serializable::BytesSerializable;
use sdk::commands::poll_messages::PollMessages;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut TcpConnection,
    command: &PollMessages,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    info!("Received an append messages command");
    let streamer = cluster.streamer.lock().await;
    let messages = streamer.poll_messages(command.offset, command.count)?;
    let mut bytes: Vec<u8> = Vec::new();
    for message in messages {
        bytes.extend(&message.as_bytes());
    }
    handler.send_ok_response(&bytes).await?;
    info!("Sent an append messages response.");
    Ok(())
}
