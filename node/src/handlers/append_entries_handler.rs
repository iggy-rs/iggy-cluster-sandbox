use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::bytes_serializable::BytesSerializable;
use sdk::commands::append_entries::AppendEntries;
use sdk::commands::create_stream;
use sdk::commands::create_stream::CreateStream;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &AppendEntries,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    info!("Received append entries command.",);
    for entry in &command.entries {
        match entry.code {
            create_stream::CREATE_STREAM_CODE => {
                let create_stream = CreateStream::from_bytes(&entry.data)?;
                cluster
                    .create_stream(command.term, create_stream.id)
                    .await?;
            }
            _ => {
                return Err(SystemError::InvalidCommand);
            }
        };
    }
    handler.send_empty_ok_response().await?;
    info!("Sent an append entries response.");
    Ok(())
}
