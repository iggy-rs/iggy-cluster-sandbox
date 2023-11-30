use crate::clients::node_client::NodeClient;
use crate::commands::command::Command;
use crate::error::SystemError;
use futures::lock::Mutex;
use monoio::time::sleep;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug)]
pub struct ClusterClient {
    clients: HashMap<String, Option<Rc<Mutex<NodeClient>>>>,
    reconnection_interval: u64,
    reconnection_retries: u32,
}

impl ClusterClient {
    pub fn new(
        addresses: Vec<&str>,
        reconnection_interval: u64,
        reconnection_retries: u32,
    ) -> Self {
        Self {
            reconnection_interval,
            reconnection_retries,
            clients: addresses
                .iter()
                .map(|address| (address.to_string(), None))
                .collect(),
        }
    }

    pub async fn init(&mut self) -> Result<(), SystemError> {
        let addresses = self.clients.keys().cloned().collect::<Vec<String>>();
        info!(
            "Connecting to Iggy cluster, nodes: {}",
            addresses.join(", ")
        );
        if self.clients.iter().all(|(_, client)| client.is_some()) {
            info!("Already connected to Iggy cluster.");
            return Ok(());
        }

        let mut retry_count = 0;
        let mut connected_clients = HashMap::new();
        for (address, client) in &self.clients {
            if client.is_some() {
                info!("Already connected to Iggy node at {address}.");
                continue;
            }

            let mut connected_client;
            loop {
                info!("Connecting to Iggy node at {address}...");
                connected_client = NodeClient::init(address).await;
                if connected_client.is_ok() {
                    break;
                }

                error!("Failed to connect to Iggy node at: {address}");
                if retry_count < self.reconnection_retries {
                    retry_count += 1;
                    info!(
                        "Retrying ({}/{}) to connect to Iggy node: {} in: {} ms...",
                        retry_count, self.reconnection_retries, address, self.reconnection_interval
                    );
                    sleep(Duration::from_millis(self.reconnection_interval)).await;
                    continue;
                }

                return Err(SystemError::CannotConnectToClusterNode(address.clone()));
            }

            connected_clients.insert(address.clone(), connected_client.unwrap());
            info!("Connected to Iggy node at {address}.");
        }

        for (address, client) in connected_clients {
            self.clients
                .insert(address, Some(Rc::new(Mutex::new(client))));
        }

        info!("Connected to Iggy cluster.");
        Ok(())
    }

    pub async fn send(&self, command: &Command) -> Result<Vec<u8>, SystemError> {
        if self.clients.iter().all(|(_, client)| client.is_none()) {
            return Err(SystemError::UnhealthyCluster);
        }

        let mut result = Vec::new();
        for (_, client) in self.clients.iter() {
            if client.is_none() {
                continue;
            }

            let client = client.as_ref().unwrap();
            let mut client = client.lock().await;
            result = client.send(command).await?
        }
        Ok(result)
    }
}
