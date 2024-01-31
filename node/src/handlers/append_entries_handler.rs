use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::append_entries::AppendEntries;
use sdk::commands::command;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::{info, warn};

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &AppendEntries,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    info!("Received append entries command.",);
    cluster
        .can_sync_state(command.leader_commit, command.prev_log_index)
        .await?;
    for entry in &command.entries {
        cluster.append_entry(entry).await?;
        match command::map_from_bytes(&entry.data)? {
            Command::CreateStream(create_stream) => {
                cluster
                    .create_stream(
                        command.term,
                        create_stream.id,
                        create_stream.replication_factor,
                    )
                    .await?;
            }
            Command::DeleteStream(delete_stream) => {
                cluster
                    .delete_stream(command.term, delete_stream.id)
                    .await?;
            }
            other => {
                warn!("Received an unknown log entry command: {other}",);
                return Err(SystemError::InvalidCommand);
            }
        };
    }
    handler.send_empty_ok_response().await?;
    info!("Sent an append entries response.");
    Ok(())
}
