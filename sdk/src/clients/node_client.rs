use crate::commands::command::Command;
use crate::error::SystemError;
use futures::lock::Mutex;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use tracing::{error, info};

const EMPTY_PAYLOAD: Vec<u8> = vec![];

#[derive(Debug)]
pub struct NodeClient {
    tcp_stream: TcpStream,
    address: String,
    is_connected: Mutex<bool>,
}

impl NodeClient {
    pub async fn init(address: &str) -> Result<Self, SystemError> {
        let tcp_stream = TcpStream::connect(address).await?;
        Ok(Self {
            tcp_stream,
            address: address.to_string(),
            is_connected: Mutex::new(true),
        })
    }

    pub async fn connect(&mut self) -> Result<(), SystemError> {
        let tcp_stream = TcpStream::connect(self.address.clone()).await?;
        self.tcp_stream = tcp_stream;
        *self.is_connected.lock().await = true;
        Ok(())
    }

    pub async fn is_connected(&self) -> bool {
        *self.is_connected.lock().await
    }

    pub async fn send(&mut self, command: &Command) -> Result<Vec<u8>, SystemError> {
        if !self.is_connected().await {
            error!(
                "Cannot send command to Iggy node at address: {}, node is not connected.",
                self.address
            );
            return Err(SystemError::CannotSendCommand);
        }

        info!(
            "Sending command to Iggy node at address: {}...",
            self.address
        );
        let (result, _) = self.tcp_stream.write_all(command.as_bytes()).await;
        if result.is_err() {
            error!(
                "Failed to send command to Iggy node at address: {}.",
                self.address
            );
            *self.is_connected.lock().await = false;
            return Err(SystemError::CannotSendCommand);
        }

        info!("Command sent to Iggy node.");
        let buffer = vec![0u8; 8];
        let (read_bytes, buffer) = self.tcp_stream.read(buffer).await;
        if read_bytes.is_err() {
            error!("Failed to read a response: {:?}", read_bytes.err());
            return Err(SystemError::CannotReadResponse);
        }

        if buffer.is_empty() {
            error!(
                "Received empty response from Iggy node at address: {}.",
                self.address
            );
            return Err(SystemError::CannotReadResponse);
        }

        let status = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        let payload_length = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        if status == 0 {
            info!("Received OK response from Iggy node at address: {}, payload length: {payload_length}.", self.address);
            if payload_length == 0 {
                return Ok(EMPTY_PAYLOAD);
            }

            let payload = vec![0u8; payload_length as usize];
            let (read_bytes, payload) = self.tcp_stream.read(payload).await;
            if read_bytes.is_err() {
                error!("Failed to read a response: {:?}", read_bytes.err());
                return Err(SystemError::CannotReadResponse);
            }

            return Ok(payload);
        }

        error!(
            "Received error response from Iggy node at address: {}, status: {status}.",
            self.address
        );
        Err(SystemError::ErrorResponse(status))
    }
}
