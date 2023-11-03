use crate::command::Command;
use crate::error::SystemError;
use futures::lock::Mutex;
use monoio::net::TcpStream;
use monoio::time::sleep;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tracing::{error, info};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ClientState {
    Disconnected,
    Connected,
}
#[derive(Debug)]
pub struct NodeClient {
    pub(crate) address: SocketAddr,
    pub(crate) stream: Mutex<Option<TcpStream>>,
    pub(crate) state: Mutex<ClientState>,
    pub(crate) reconnection_retries: u32,
    pub(crate) reconnection_interval: u64,
}

impl NodeClient {
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
        let elapsed;
        loop {
            info!("Connecting to cluster node: {}...", self.address);
            let now = Instant::now();
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
            elapsed = now.elapsed();
            break;
        }

        info!(
            "Connected to cluster node: {remote_address} in {} ms.",
            elapsed.as_millis()
        );
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
        let now = Instant::now();
        self.send_request(Command::Ping).await?;
        let elapsed = now.elapsed();
        info!(
            "Received a pong from cluster node: {} in {} ms.",
            self.address,
            elapsed.as_millis()
        );
        Ok(())
    }

    pub async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    pub async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }
}
