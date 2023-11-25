use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use tracing::{error, info};

#[derive(Debug)]
pub(crate) struct Client {
    tcp_stream: TcpStream,
}

impl Client {
    pub async fn init(address: &str) -> Result<Self, SystemError> {
        let tcp_stream = TcpStream::connect(address).await?;
        Ok(Self { tcp_stream })
    }

    pub async fn send(&mut self, command: Command) -> Result<(), SystemError> {
        info!("Sending command to Iggy node...");
        let (result, _) = self.tcp_stream.write_all(command.as_bytes()).await;
        if result.is_err() {
            error!("Failed to send command to Iggy node.");
            return Err(SystemError::CannotSendCommand);
        }

        info!("Command sent to Iggy node.");
        let buffer = vec![0u8; 8];
        let (read_bytes, buffer) = self.tcp_stream.read(buffer).await;
        if read_bytes.is_err() {
            error!("Failed to read a response: {:?}", read_bytes.err());
            return Err(SystemError::CannotReadResponse);
        }

        let status = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        let payload_length = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        if status == 0 {
            info!("Received OK response from Iggy node, payload length: {payload_length}.",);
            return Ok(());
        }

        error!("Received error response from Iggy node, status: {status}.");
        Err(SystemError::ErrorResponse(status))
    }
}
