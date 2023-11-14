use crate::commands::ping::Ping;
use crate::connection::tcp_handler::TcpHandler;
use crate::error::SystemError;
use futures::lock::Mutex;
use monoio::net::TcpStream;
use monoio::time::sleep;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tracing::{error, info};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ClientState {
    Disconnected,
    Connected,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum HealthState {
    Unknown,
    Healthy,
    Unhealthy,
}

#[derive(Debug)]
pub struct NodeClient {
    pub(crate) address: SocketAddr,
    pub(crate) handler: Mutex<Option<TcpHandler>>,
    client_state: Mutex<ClientState>,
    health_state: Mutex<HealthState>,
    reconnection_retries: u32,
    reconnection_interval: u64,
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
            handler: Mutex::new(None),
            client_state: Mutex::new(ClientState::Disconnected),
            health_state: Mutex::new(HealthState::Unknown),
            reconnection_retries,
            reconnection_interval,
        })
    }

    pub async fn connect(&self) -> Result<(), SystemError> {
        if self.get_client_state().await == ClientState::Connected {
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

            elapsed = now.elapsed();
            let stream = connection.unwrap();
            remote_address = stream.peer_addr()?;
            self.handler.lock().await.replace(TcpHandler::new(stream));
            self.set_client_state(ClientState::Connected).await;
            self.set_health_state(HealthState::Healthy).await;
            break;
        }

        info!(
            "Connected to cluster node: {remote_address} in {} ms.",
            elapsed.as_millis()
        );
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        if self.get_client_state().await == ClientState::Disconnected {
            return Ok(());
        }

        let health_state = self.get_health_state().await;
        info!(
            "Disconnecting from cluster node: {}, health state: {health_state}...",
            self.address
        );
        self.set_client_state(ClientState::Disconnected).await;
        self.set_health_state(HealthState::Unknown).await;
        self.handler.lock().await.take();
        info!("Disconnected from  cluster node: {}.", self.address);
        Ok(())
    }

    pub async fn ping(&self) -> Result<(), SystemError> {
        info!("Sending a ping to cluster node: {}...", self.address);
        let now = Instant::now();
        if let Err(error) = self.send_request(&Ping::new_command()).await {
            error!("Failed to send a ping to cluster node: {}", self.address);
            self.set_health_state(HealthState::Unhealthy).await;
            self.set_client_state(ClientState::Disconnected).await;
            return Err(error);
        }
        let elapsed = now.elapsed();
        info!(
            "Received a pong from cluster node: {} in {} ms.",
            self.address,
            elapsed.as_millis()
        );
        Ok(())
    }

    pub async fn get_client_state(&self) -> ClientState {
        *self.client_state.lock().await
    }

    async fn set_client_state(&self, state: ClientState) {
        *self.client_state.lock().await = state;
    }

    pub async fn get_health_state(&self) -> HealthState {
        *self.health_state.lock().await
    }

    async fn set_health_state(&self, state: HealthState) {
        *self.health_state.lock().await = state;
    }
}

impl Display for ClientState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientState::Disconnected => write!(f, "disconnected"),
            ClientState::Connected => write!(f, "connected"),
        }
    }
}

impl Display for HealthState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthState::Unknown => write!(f, "unknown"),
            HealthState::Healthy => write!(f, "healthy"),
            HealthState::Unhealthy => write!(f, "unhealthy"),
        }
    }
}
