use crate::error::SystemError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

const STATUS_OK: &[u8] = &[0; 4];

#[derive(Debug)]
pub struct TcpSender {
    pub(crate) stream: TcpStream,
}

unsafe impl Send for TcpSender {}
unsafe impl Sync for TcpSender {}

impl TcpSender {
    pub async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, SystemError> {
        let read_bytes = self.stream.read_exact(buffer).await;
        if let Err(error) = read_bytes {
            return Err(SystemError::from(error));
        }

        Ok(read_bytes.unwrap())
    }

    pub async fn send_empty_ok_response(&mut self) -> Result<(), SystemError> {
        self.send_ok_response(&[]).await
    }

    pub async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), SystemError> {
        self.send_response(STATUS_OK, payload).await
    }

    pub async fn send_response(
        &mut self,
        status: &[u8],
        payload: &[u8],
    ) -> Result<(), SystemError> {
        debug!("Sending response with status: {:?}...", status);
        let length = (payload.len() as u32).to_le_bytes();
        self.stream
            .write_all(&[status, &length, payload].as_slice().concat())
            .await?;
        debug!("Sent response with status: {:?}", status);
        Ok(())
    }
}
