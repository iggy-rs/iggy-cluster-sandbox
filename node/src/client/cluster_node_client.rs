use crate::error::SystemError;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info};

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

    pub async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    pub async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }
}
