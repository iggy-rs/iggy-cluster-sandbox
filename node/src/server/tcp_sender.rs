use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use crate::error::SystemError;
use tracing::debug;

const STATUS_OK: &[u8] = &[0; 4];

#[derive(Debug)]
pub struct TcpSender {
    pub(crate) stream: TcpStream
}

unsafe impl Send for TcpSender {}
unsafe impl Sync for TcpSender {}

impl TcpSender {
    pub async fn read(&mut self, buffer: Vec<u8>) -> Result<(usize, Vec<u8>), SystemError> {
        let (read_bytes, buffer) = self.stream.read(buffer).await;
        if let Err(error) = read_bytes {
            return Err(SystemError::from(error));
        }

        Ok((read_bytes.unwrap(), buffer))
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
        let mut buffer = Vec::with_capacity(status.len() + length.len() + payload.len());
        buffer.extend(status);
        buffer.extend(&length);
        buffer.extend(payload);
        let result = self.stream
            .write_all(buffer)
            .await;
        if result.0.is_err() {
            return Err(SystemError::from(result.0.unwrap_err()));
        }
        debug!("Sent response with status: {:?}", status);
        Ok(())
    }
}
