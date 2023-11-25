use bytes::BufMut;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use tracing::{debug, error};

const STATUS_OK: u32 = 0;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const EMPTY_BYTES: Vec<u8> = vec![];

#[derive(Debug)]
pub(crate) struct TcpConnection {
    stream: TcpStream,
}

impl TcpConnection {
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

    pub async fn send_request(
        &mut self,
        command: &Command,
    ) -> Result<(usize, Vec<u8>), SystemError> {
        self.send(command.as_bytes(), true).await
    }

    pub async fn send_error_response(&mut self, error: SystemError) -> Result<(), SystemError> {
        let mut data = Vec::with_capacity(8);
        data.put_u32_le(error.as_code());
        data.put_u32_le(0);
        self.send(data, false).await?;
        Ok(())
    }

    pub async fn send_empty_ok_response(&mut self) -> Result<(), SystemError> {
        self.send_ok_response(&[]).await
    }

    pub async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), SystemError> {
        let mut data = Vec::with_capacity(8 + payload.len());
        data.put_u32_le(STATUS_OK);
        data.put_u32_le(payload.len() as u32);
        data.extend(payload);
        self.send(data, false).await?;
        Ok(())
    }

    async fn send(
        &mut self,
        payload: Vec<u8>,
        read_response: bool,
    ) -> Result<(usize, Vec<u8>), SystemError> {
        let payload_length = payload.len();
        debug!("Sending data with payload length: {payload_length}...");
        let result = self.stream.write_all(payload).await;
        if result.0.is_err() {
            return Err(SystemError::from(result.0.unwrap_err()));
        }

        debug!("Sent data with payload length: {payload_length}.");
        if !read_response {
            return Ok((0, EMPTY_BYTES));
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

        let payload_length = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        debug!("Received a response with status: {status}, payload length: {payload_length}.");
        if payload_length == 0 {
            return Ok((0, EMPTY_BYTES));
        }

        self.read(vec![0u8; payload_length as usize]).await
    }
}
