use crate::command::Command;
use crate::error::SystemError;
use bytes::BufMut;
use futures::lock::Mutex;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use monoio::time::sleep;
use std::net::SocketAddr;
use std::time::Duration;
use tracing::{error, info, warn};

const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ClientState {
    Disconnected,
    Connected,
}
#[derive(Debug)]
pub struct ClusterNodeClient {
    pub(crate) address: SocketAddr,
    pub(crate) stream: Mutex<Option<TcpStream>>,
    pub(crate) state: Mutex<ClientState>,
    pub(crate) reconnection_retries: u32,
    pub(crate) reconnection_interval: u64,
}

impl ClusterNodeClient {
    pub fn new(
        address: &str,
        reconnection_retries: u32,
        reconnection_interval: u64,
    ) -> Result<Self, SystemError> {
        let node_address = address;
        let address = address.parse::<SocketAddr>();
        if address.is_err() {
            return Err(SystemError::InvalidClusterNodeAddress(
                node_address.to_string(),
            ));
        }

        Ok(Self {
            address: address.unwrap(),
            stream: Mutex::new(None),
            state: Mutex::new(ClientState::Disconnected),
            reconnection_retries,
            reconnection_interval,
        })
    }

    pub async fn connect(&self) -> Result<(), SystemError> {
        if self.get_state().await == ClientState::Connected {
            return Ok(());
        }

        let mut retry_count = 0;
        let remote_address;
        loop {
            info!("Connecting to cluster node: {}...", self.address);
            let connection = TcpStream::connect(self.address).await;
            if connection.is_err() {
                error!("Failed to connect to cluster node: {}", self.address);
                if retry_count < self.reconnection_retries {
                    retry_count += 1;
                    info!(
                        "Retrying ({}/{}) to connect to cluster node: {} in: {} ms...",
                        retry_count,
                        self.reconnection_retries,
                        self.address,
                        self.reconnection_interval
                    );
                    sleep(Duration::from_millis(self.reconnection_interval)).await;
                    continue;
                }

                return Err(SystemError::CannotConnectToClusterNode(
                    self.address.to_string(),
                ));
            }

            let stream = connection.unwrap();
            remote_address = stream.peer_addr()?;
            self.stream.lock().await.replace(stream);
            self.set_state(ClientState::Connected).await;
            break;
        }

        info!("Connected to cluster node: {remote_address}");
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        if self.get_state().await == ClientState::Disconnected {
            return Ok(());
        }

        info!("Disconnecting from cluster node: {}...", self.address);
        self.set_state(ClientState::Disconnected).await;
        self.stream.lock().await.take();
        info!("Disconnected from  cluster node: {}.", self.address);
        Ok(())
    }

    pub async fn ping(&self) -> Result<(), SystemError> {
        info!("Sending a ping to cluster node: {}...", self.address);
        self.send_request(Command::Ping).await?;
        info!("Received a pong from cluster node: {}.", self.address);
        Ok(())
    }

    async fn send_request(&self, command: Command) -> Result<(), SystemError> {
        let state = self.get_state().await;
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
            self.set_state(ClientState::Disconnected).await;
            return Err(SystemError::ClientDisconnected);
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

    pub async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    pub async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }
}
