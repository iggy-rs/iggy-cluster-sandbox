use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use log::warn;
use sdk::commands::append_entries::AppendEntries;
use sdk::commands::command;
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
        match command::map_from_bytes(&entry.data)? {
            command::Command::CreateStream(create_stream) => {
                cluster
                    .create_stream(command.term, create_stream.id)
                    .await?;
            }
            other => {
                warn!("Received an unknown log entry command: {other}",);
                return Err(SystemError::InvalidCommand);
            }
        };
    }
    cluster.sync_state(&command.entries).await?;
    handler.send_empty_ok_response().await?;
    info!("Sent an append entries response.");
    Ok(())
}
