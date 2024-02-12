use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use crate::handlers::*;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::debug;

pub async fn handle(
    handler: &mut ConnectionHandler,
    command: &Command,
    cluster: &Rc<Cluster>,
) -> Result<(), SystemError> {
    let command_name = command.get_name();
    let cluster = cluster.clone();
    debug!("Handling a TCP request, command: {command_name}...");
    match command {
        Command::Hello(command) => {
            hello_handler::handle(handler, command, cluster).await?;
        }
        Command::Heartbeat(command) => {
            heartbeat_handler::handle(handler, command, cluster).await?;
        }
        Command::Ping(_) => {
            ping_handler::handle(handler, cluster).await?;
        }
        Command::RequestVote(command) => {
            request_vote_handler::handle(handler, command, cluster).await?;
        }
        Command::UpdateLeader(command) => {
            update_leader_handler::handle(handler, command, cluster).await?;
        }
        Command::GetNodeState(_) => {
            get_node_state_handler::handle(handler, cluster).await?;
        }
        Command::LoadState(command) => {
            load_state_handler::handle(handler, command, cluster).await?;
        }
        Command::GetMetadata(_) => {
            get_metadata_handler::handle(handler, cluster).await?;
        }
        Command::GetStreams(_) => {
            get_streams_handler::handle(handler, cluster).await?;
        }
        Command::CreateStream(command) => {
            create_stream_handler::handle(handler, command, cluster).await?;
        }
        Command::DeleteStream(command) => {
            delete_stream_handler::handle(handler, command, cluster).await?;
        }
        Command::AppendMessages(command) => {
            append_messages_handler::handle(handler, command, cluster).await?;
        }
        Command::PollMessages(command) => {
            poll_messages_handler::handle(handler, command, cluster).await?;
        }
        Command::SyncMessages(command) => {
            sync_messages_handler::handle(handler, command, cluster).await?;
        }
        Command::AppendEntries(command) => {
            append_entries_handler::handle(handler, command, cluster).await?;
        }
    }
    debug!("Handled a TCP request, command: {command_name}.");
    Ok(())
}
