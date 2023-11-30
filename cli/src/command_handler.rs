use bytes::Bytes;
use sdk::clients::cluster_client::ClusterClient;
use sdk::commands::command::Command;
use std::fmt::{Display, Formatter};
use tracing::{error, info};

pub(crate) async fn handle(command: &Command, client: &ClusterClient) {
    let result = client.send(command).await;
    if result.is_err() {
        error!("There was an error sending the command to the cluster.");
    }

    let result = result.unwrap();
    if let Command::PollMessages(_) = command {
        handle_poll_messages(&result);
    }
}

fn handle_poll_messages(bytes: &[u8]) {
    let mut messages = Vec::new();
    let mut position = 0;
    while position < bytes.len() {
        let offset = u64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
        let id = u64::from_le_bytes(bytes[position + 8..position + 16].try_into().unwrap());
        let payload_length =
            u32::from_le_bytes(bytes[position + 16..position + 20].try_into().unwrap());
        let payload =
            Bytes::from(bytes[position + 20..position + 20 + payload_length as usize].to_vec());
        position += 20 + payload_length as usize;
        let message = Message {
            offset,
            id,
            payload,
        };
        messages.push(message);
    }

    info!("Polled {} messages", messages.len());

    for message in messages {
        info!("{message}");
    }
}

#[derive(Debug)]
struct Message {
    pub offset: u64,
    pub id: u64,
    pub payload: Bytes,
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Message {{ offset: {}, id: {}, payload: {} }}",
            self.offset,
            self.id,
            String::from_utf8_lossy(&self.payload)
        )
    }
}
