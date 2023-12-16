use crate::clusters::cluster::SelfNode;
use crate::clusters::node::Resiliency;
use crate::connection::handler::ConnectionHandler;
use futures::lock::Mutex;
use monoio::net::TcpStream;
use monoio::time::sleep;
use sdk::commands::hello::Hello;
use sdk::commands::ping::Ping;
use sdk::commands::request_vote::RequestVote;
use sdk::commands::update_leader::UpdateLeader;
use sdk::error::SystemError;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

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

impl SelfNode {
    pub fn new(id: u64, name: &str, address: &str) -> Self {
        Self {
            id,
            name: name.to_string(),
            address: address.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct NodeClient {
    pub id: u64,
    pub secret: String,
    pub self_node: SelfNode,
    pub address: SocketAddr,
    pub handler: Mutex<Option<ConnectionHandler>>,
    client_state: Mutex<ClientState>,
    health_state: Mutex<HealthState>,
    resiliency: Resiliency,
}

impl NodeClient {
    pub fn new(
        id: u64,
        secret: &str,
        self_node: SelfNode,
        address: &str,
        resiliency: Resiliency,
    ) -> Result<Self, SystemError> {
        let node_address = address;
        let address = address.parse::<SocketAddr>();
        if address.is_err() {
            return Err(SystemError::InvalidClusterNodeAddress(
                node_address.to_string(),
            ));
        }

        Ok(Self {
            id,
            secret: secret.to_string(),
            self_node,
            address: address.unwrap(),
            handler: Mutex::new(None),
            client_state: Mutex::new(ClientState::Disconnected),
            health_state: Mutex::new(HealthState::Unknown),
            resiliency,
        })
    }

    pub fn is_self_node(&self) -> bool {
        self.id == self.self_node.id
    }

    pub async fn connect(&self) -> Result<(), SystemError> {
        if self.get_client_state().await == ClientState::Connected {
            warn!(
                "Already connected to cluster node ID: {}, address: {}",
                self.id, self.address
            );
            return Ok(());
        }

        let mut retry_count = 0;
        let remote_address;
        let elapsed;
        loop {
            info!(
                "Connecting to cluster node ID: {}, address: {}...",
                self.id, self.address
            );
            let now = Instant::now();
            let connection = TcpStream::connect(self.address).await;
            if connection.is_err() {
                error!(
                    "Failed to connect to cluster node ID: {}, address: {}.",
                    self.id, self.address
                );
                if retry_count < self.resiliency.reconnection_retries {
                    retry_count += 1;
                    info!(
                        "Retrying ({}/{}) to connect to cluster node ID: {}, address: {}, in: {} ms...",
                        retry_count,
                        self.resiliency.reconnection_retries,
                        self.id,
                        self.address,
                        self.resiliency.reconnection_interval
                    );
                    sleep(Duration::from_millis(self.resiliency.reconnection_interval)).await;
                    continue;
                }

                return Err(SystemError::CannotConnectToClusterNode(
                    self.address.to_string(),
                ));
            }

            elapsed = now.elapsed();
            let stream = connection.unwrap();
            remote_address = stream.peer_addr()?;
            self.handler
                .lock()
                .await
                .replace(ConnectionHandler::new(stream, self.id));
            self.set_client_state(ClientState::Connected).await;
            self.set_health_state(HealthState::Healthy).await;
            break;
        }

        info!(
            "Connected to cluster node ID: {}, address: {remote_address} in {} ms. Sending hello message...",
            self.id,
            elapsed.as_millis()
        );

        self.send_request(&Hello::new_command(
            self.secret.clone(),
            self.self_node.name.clone(),
            self.self_node.id,
        ))
        .await?;
        info!(
            "Sent hello message to cluster node ID: {}, address: {}",
            self.id, self.address
        );
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), SystemError> {
        if self.get_client_state().await == ClientState::Disconnected {
            return Ok(());
        }

        let health_state = self.get_health_state().await;
        info!(
            "Disconnecting from cluster node ID: {}, address: {}, health state: {health_state}...",
            self.id, self.address
        );
        self.set_client_state(ClientState::Disconnected).await;
        self.set_health_state(HealthState::Unknown).await;
        self.handler.lock().await.take();
        info!(
            "Disconnected from  cluster node ID: {}, address: {}.",
            self.id, self.address
        );
        Ok(())
    }

    pub async fn ping(&self) -> Result<(), SystemError> {
        debug!(
            "Sending a ping to cluster node ID: {}, address: {}...",
            self.id, self.address
        );
        let now = Instant::now();
        if let Err(error) = self.send_request(&Ping::new_command()).await {
            error!(
                "Failed to send a ping to cluster node ID: {}, address: {}",
                self.id, self.address
            );
            self.set_health_state(HealthState::Unhealthy).await;
            self.set_client_state(ClientState::Disconnected).await;
            return Err(error);
        }
        let elapsed = now.elapsed();
        debug!(
            "Received a pong from cluster node ID: {}, address: {} in {} ms.",
            self.id,
            self.address,
            elapsed.as_millis()
        );
        Ok(())
    }

    pub async fn request_vote(&self, term: u64) -> Result<(), SystemError> {
        info!(
            "Sending a request vote to cluster node ID: {}, address: {}, term: {}...",
            self.id, self.address, term
        );
        let command = RequestVote::new_command(term);
        self.send_request(&command).await?;
        if let Err(error) = self.send_request(&command).await {
            error!(
                "Failed to send a request vote to cluster node ID: {}, address: {}, term: {}.",
                self.id, self.address, term
            );
            return Err(error);
        }
        info!(
            "Received a request vote response from cluster node ID: {}, address: {}, term: {}.",
            self.id, self.address, term
        );
        Ok(())
    }

    pub async fn update_leader(&self, term: u64) -> Result<(), SystemError> {
        info!(
            "Sending an update leader to cluster node ID: {}, address: {}, term: {}...",
            self.id, self.address, term
        );
        let command = UpdateLeader::new_command(term);
        self.send_request(&command).await?;
        if let Err(error) = self.send_request(&command).await {
            error!(
                "Failed to send an update leader to cluster node ID: {}, address: {}, term: {}.",
                self.id, self.address, term
            );
            return Err(error);
        }
        info!(
            "Received an update leader response from cluster node ID: {}, address: {}, term: {}.",
            self.id, self.address, term
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
