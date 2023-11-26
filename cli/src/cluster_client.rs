use crate::client::Client;
use futures::lock::Mutex;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use std::collections::HashMap;
use std::rc::Rc;
use tracing::info;

#[derive(Debug)]
pub(crate) struct ClusterClient {
    clients: HashMap<String, Option<Rc<Mutex<Client>>>>,
}

impl ClusterClient {
    pub fn new(addresses: Vec<String>) -> Self {
        Self {
            clients: addresses
                .iter()
                .map(|address| (address.clone(), None))
                .collect(),
        }
    }

    pub async fn init(&mut self) -> Result<(), SystemError> {
        if self.clients.iter().all(|(_, client)| client.is_some()) {
            info!("Already connected to all Iggy nodes.");
            return Ok(());
        }

        let mut connected_clients = HashMap::new();
        for (address, client) in &self.clients {
            if client.is_some() {
                info!("Already connected to Iggy node at {address}.");
                continue;
            }

            info!("Connecting to Iggy node at {address}...");
            let connected_client = Client::init(address).await?;
            connected_clients.insert(address.clone(), connected_client);
            info!("Connected to Iggy node at {address}.");
        }

        for (address, client) in connected_clients {
            self.clients
                .insert(address, Some(Rc::new(Mutex::new(client))));
        }

        Ok(())
    }

    pub async fn send(&self, command: &Command) -> Result<(), SystemError> {
        for (_, client) in self.clients.iter() {
            if client.is_none() {
                continue;
            }

            let client = client.as_ref().unwrap();
            let mut client = client.lock().await;
            client.send(command).await?
        }
        Ok(())
    }
}
