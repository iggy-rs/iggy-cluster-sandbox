use crate::command::Command;
use crate::error::SystemError;
use bytes::BufMut;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use tracing::{debug, error};

const STATUS_OK: u32 = 0;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const EMPTY_BYTES: Vec<u8> = vec![];

#[derive(Debug)]
pub struct TcpHandler {
    stream: TcpStream,
}

impl TcpHandler {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub async fn read(&mut self, buffer: Vec<u8>) -> Result<(usize, Vec<u8>), SystemError> {
        let (read_bytes, buffer) = self.stream.read(buffer).await;
        if let Err(error) = read_bytes {
            return Err(SystemError::from(error));
        }

        Ok((read_bytes.unwrap(), buffer))
    }

    pub async fn send_request(&mut self, command: &Command) -> Result<Vec<u8>, SystemError> {
        self.send(command.as_code(), &command.as_bytes(), true)
            .await
    }

    pub async fn send_empty_ok_response(&mut self) -> Result<(), SystemError> {
        self.send_ok_response(&[]).await
    }

    pub async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), SystemError> {
        self.send(STATUS_OK, payload, false).await?;
        Ok(())
    }

    async fn send(
        &mut self,
        code: u32,
        payload: &[u8],
        read_response: bool,
    ) -> Result<Vec<u8>, SystemError> {
        debug!("Sending data with code: {code}...");
        let payload_length = payload.len();
        let mut buffer = Vec::with_capacity(4 + payload_length + payload.len());
        buffer.put_u32_le(code);
        buffer.put_u32_le(payload_length as u32);
        buffer.extend(payload);
        debug!("Sending data with code: {code}...");
        let result = self.stream.write_all(buffer).await;
        if result.0.is_err() {
            return Err(SystemError::from(result.0.unwrap_err()));
        }

        debug!("Sent data with code: {code}");
        if !read_response {
            return Ok(EMPTY_BYTES);
        }

        let buffer = vec![0u8; RESPONSE_INITIAL_BYTES_LENGTH];
        let (read_bytes, buffer) = self.stream.read(buffer).await;
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

        Ok(EMPTY_BYTES)
    }
}
