use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::bytes_serializable::BytesSerializable;
use sdk::commands::poll_messages::PollMessages;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &PollMessages,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    cluster.verify_is_leader().await?;
    let streamer = cluster.streamer.lock().await;
    let messages = streamer.poll_messages(command.stream_id, command.offset, command.count)?;
    let mut bytes: Vec<u8> = Vec::new();
    for message in messages {
        bytes.extend(&message.as_bytes());
    }
    handler.send_ok_response(&bytes).await?;
    Ok(())
}
