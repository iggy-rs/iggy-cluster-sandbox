use crate::clusters::node_client::{ClientState, NodeClient};
use sdk::commands::command::Command;
use sdk::error::SystemError;
use tracing::{debug, warn};

impl NodeClient {
    pub async fn send_request(&self, command: &Command) -> Result<(), SystemError> {
        let state = self.get_client_state().await;
        if state == ClientState::Disconnected {
            warn!("Cannot send a request, client is disconnected.");
            return Err(SystemError::ClientDisconnected);
        }

        let mut stream = self.handler.lock().await;
        if stream.is_none() {
            warn!("Cannot send a request, client is disconnected.");
            return Err(SystemError::ClientDisconnected);
        }

        debug!(
            "Sending a request to cluster node with address: {}...",
            self.address
        );
        let handler = stream.as_mut().unwrap();
        let result = handler.send_request(command).await;
        if result.is_err() {
            debug!("Failed to send a request: {:?}", result.err());
            return Err(SystemError::SendRequestFailed);
        }

        debug!(
            "Sent a request to cluster node with address: {}.",
            self.address
        );
        Ok(())
    }
}
