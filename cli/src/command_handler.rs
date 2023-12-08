use sdk::clients::cluster_client::ClusterClient;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use tracing::info;

pub(crate) async fn handle(command: Command, client: &ClusterClient) -> Result<(), SystemError> {
    match command {
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
        Command::Ping(_) => {
            client.ping().await?;
            info!("Pinged cluster");
        }
        Command::GetMetadata(_) => {
            let metadata = client.get_metadata().await?;
            info!("Metadata: {metadata}");
        }
        Command::CreateStream(create_stream) => {
            client.create_stream(create_stream.id).await?;
            info!("Created stream {}", create_stream.id);
        }
        _ => {
            return Err(SystemError::InvalidCommand);
        }
    }
    Ok(())
}
