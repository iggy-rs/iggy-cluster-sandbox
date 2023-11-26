use crate::clusters::cluster::Cluster;
use crate::connection::tcp_connection::TcpConnection;
use crate::handlers::*;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

pub async fn handle(
    handler: &mut TcpConnection,
    command: &Command,
    cluster: &Rc<Cluster>,
) -> Result<(), SystemError> {
    let command_name = command.get_name();
    let cluster = cluster.clone();
    info!("Handling a TCP request, command: {command_name}...");
    match command {
        Command::Hello(command) => {
            hello_handler::handle(handler, command, cluster).await?;
        }
        Command::Ping(_) => {
            ping_handler::handle(handler).await?;
        }
        Command::AppendMessages(command) => {
            append_messages_handler::handle(handler, command, cluster).await?;
        }
        Command::SyncMessages(command) => {
            sync_messages_handler::handle(handler, command, cluster).await?;
        }
    }
    info!("Handled a TCP request, command: {command_name}.");
    Ok(())
}