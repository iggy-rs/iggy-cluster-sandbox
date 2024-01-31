use sdk::clients::cluster_client::ClusterClient;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use tracing::info;

pub(crate) async fn handle(command: Command, client: &ClusterClient) -> Result<(), SystemError> {
    match command {
        Command::Ping(_) => {
            client.ping().await?;
            info!("Pinged the cluster");
        }
        Command::PollMessages(poll_messages) => {
            let messages = client
                .poll_messages(
                    poll_messages.stream_id,
                    poll_messages.offset,
                    poll_messages.count,
                )
                .await?;
            info!("Polled {} messages", messages.len());
            for message in messages {
                info!("{message}");
            }
        }
        Command::AppendMessages(append_messages) => {
            let count = append_messages.messages.len();
            client
                .append_messages(append_messages.stream_id, append_messages.messages)
                .await?;
            info!("Appended {count} messages");
        }
        Command::GetMetadata(_) => {
            client.update_metadata().await?;
        }
        Command::GetStreams(_) => {
            let streams = client.get_streams().await?;
            info!("Got {} streams", streams.len());
            for stream in streams {
                info!("{stream}");
            }
        }
        Command::CreateStream(create_stream) => {
            client
                .create_stream(create_stream.id, create_stream.replication_factor)
                .await?;
            info!("Created stream with ID: {}", create_stream.id);
        }
        Command::DeleteStream(delete_stream) => {
            client.delete_stream(delete_stream.id).await?;
            info!("Deleted stream with ID: {}", delete_stream.id);
        }
        _ => {
            return Err(SystemError::InvalidCommand);
        }
    }
    Ok(())
}
