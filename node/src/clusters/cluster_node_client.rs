use crate::command::Command;
use crate::error::SystemError;
use bytes::BufMut;
use std::net::SocketAddr;
use std::time::Duration;
use futures::lock::Mutex;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use monoio::time::sleep;
use tracing::{error, info, warn};

const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ClientState {
    Disconnected,
    Connected,
}
#[derive(Debug)]
pub struct ClusterNodeClient {
    pub(crate) node_address: SocketAddr,
    pub(crate) stream: Mutex<Option<TcpStream>>,
    pub(crate) state: Mutex<ClientState>,
    pub(crate) reconnection_retries: u32,
    pub(crate) reconnection_interval: u64,
}

impl ClusterNodeClient {
    pub fn new(
        node_address: &str,
        reconnection_retries: u32,
        reconnection_interval: u64,
    ) -> Result<Self, SystemError> {
        let address = node_address;
        let node_address = node_address.parse::<SocketAddr>();
        if node_address.is_err() {
            return Err(SystemError::InvalidClusterNodeAddress(address.to_string()));
        }

        Ok(Self {
            node_address: node_address.unwrap(),
            stream: Mutex::new(None),
            state: Mutex::new(ClientState::Disconnected),
            reconnection_retries,
            reconnection_interval,
        })
    }

    pub fn new_with_defaults(node_address: &str) -> Result<Self, SystemError> {
        Self::new(node_address, 10, 1000)
    }

    pub async fn connect(&self) -> Result<(), SystemError> {
        if self.get_state().await == ClientState::Connected {
            return Ok(());
        }

        let mut retry_count = 0;
        let remote_address;
        loop {
            info!("Connecting to cluster node: {}...", self.node_address);
            let connection = TcpStream::connect(self.node_address).await;
            if connection.is_err() {
                error!("Failed to connect to cluster node: {}", self.node_address);
                if retry_count < self.reconnection_retries {
                    retry_count += 1;
                    info!(
                        "Retrying ({}/{}) to connect to cluster node: {} in: {} ms...",
                        retry_count,
                        self.reconnection_retries,
                        self.node_address,
                        self.reconnection_interval
                    );
                    sleep(Duration::from_millis(self.reconnection_interval)).await;
                    continue;
                }

                return Err(SystemError::CannotConnectToClusterNode(
                    self.node_address.to_string(),
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

        info!("Disconnecting from cluster node: {}...", self.node_address);
        self.set_state(ClientState::Disconnected).await;
        self.stream.lock().await.take();
        info!("Disconnected from  cluster node: {}.", self.node_address);
        Ok(())
    }

    pub async fn start_healthcheck(&self) {
        let interval = Duration::from_secs(3);
        let mut buffer: Vec<u8> = Vec::with_capacity(8 * 1024);
        let mut read_bytes;
        loop {
            sleep(interval).await;
            let state = self.get_state().await;
            if state == ClientState::Disconnected {
                warn!("Cannot send a ping, client is disconnected.");
                continue;
            }

            let mut stream = self.stream.lock().await;
            if stream.is_none() {
                warn!("Cannot send a ping, client is disconnected.");
                continue;
            }

            info!("Sending a ping to cluster node: {}...", self.node_address);
            let stream = stream.as_mut().unwrap();
            let command = Command::Ping;
            let command_bytes = command.as_bytes();
            let mut payload = Vec::with_capacity(8 + command_bytes.len());
            payload.put_u32_le(command.as_code());
            payload.put_u32_le(command_bytes.len() as u32);
            payload.extend_from_slice(&command_bytes);
            let (write_result, _) = stream.write_all(payload).await;
            if write_result.is_err() {
                error!("Failed to send a ping: {:?}", write_result.err());
                continue;
            }

            // let mut buffer = [0u8; RESPONSE_INITIAL_BYTES_LENGTH];
            (read_bytes, buffer) = stream.read(buffer).await;
            if read_bytes.is_err() {
                error!("Failed to read a ping response: {:?}", read_bytes.err());
                continue;
            }

            let read_bytes = read_bytes.unwrap();
            if read_bytes != RESPONSE_INITIAL_BYTES_LENGTH {
                error!("Received an invalid or empty response.");
                continue;
            }

            let status = u32::from_le_bytes(buffer[..4].try_into().unwrap());
            if status != 0 {
                error!("Received an invalid ping response with status: {status}.");
                continue;
            }

            let length = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
            info!("Received a valid ping response with length: {length}.");
        }
    }

    pub async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    pub async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }
}
