use crate::clusters::node_client::{ClientState, NodeClient};
use crate::command::Command;
use crate::error::SystemError;
use bytes::BufMut;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use tracing::{error, info, warn};

const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;

impl NodeClient {
    pub async fn send_request(&self, command: Command) -> Result<(), SystemError> {
        let state = self.get_client_state().await;
        if state == ClientState::Disconnected {
            warn!("Cannot send a request, client is disconnected.");
            return Err(SystemError::ClientDisconnected);
        }

        let mut stream = self.stream.lock().await;
        if stream.is_none() {
            warn!("Cannot send a request, client is disconnected.");
            return Err(SystemError::ClientDisconnected);
        }

        info!("Sending a request to cluster node: {}...", self.address);
        let stream = stream.as_mut().unwrap();
        let command_bytes = command.as_bytes();
        let mut payload = Vec::with_capacity(8 + command_bytes.len());
        payload.put_u32_le(command.as_code());
        payload.put_u32_le(command_bytes.len() as u32);
        payload.extend_from_slice(&command_bytes);
        let (write_result, _) = stream.write_all(payload).await;
        if write_result.is_err() {
            error!("Failed to send a request: {:?}", write_result.err());
            return Err(SystemError::SendRequestFailed);
        }

        let buffer = vec![0u8; RESPONSE_INITIAL_BYTES_LENGTH];
        let (read_bytes, buffer) = stream.read(buffer).await;
        if read_bytes.is_err() {
            error!("Failed to read a response: {:?}", read_bytes.err());
            return Err(SystemError::InvalidResponse);
        }

        let read_bytes = read_bytes.unwrap();
        if read_bytes != RESPONSE_INITIAL_BYTES_LENGTH {
            error!("Received an invalid response.");
            return Err(SystemError::InvalidResponse);
        }

        let status = u32::from_le_bytes(buffer[..4].try_into().unwrap());
        if status != 0 {
            error!("Received an invalid response with status: {status}.");
            return Err(SystemError::InvalidResponse);
        }

        Ok(())
    }
}
