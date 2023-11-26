mod client;
mod cluster_client;
mod command_parser;

use crate::cluster_client::ClusterClient;
use figlet_rs::FIGfont;
use monoio::time::sleep;
use sdk::error::SystemError;
use std::time::Duration;
use std::{env, io};
use tracing::{error, info};

#[monoio::main(timer_enabled = true)]
async fn main() -> Result<(), SystemError> {
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy CLI");
    println!("{}", figure.unwrap());
    let reconnection_interval = 1000;
    let address1 = env::var("IGGY_NODE1_ADDRESS").unwrap_or("127.0.0.1:8101".to_string());
    let address2 = env::var("IGGY_NODE2_ADDRESS").unwrap_or("127.0.0.1:8102".to_string());
    let address3 = env::var("IGGY_NODE3_ADDRESS").unwrap_or("127.0.0.1:8103".to_string());
    let addresses = vec![address1, address2, address3];
    let mut cluster = ClusterClient::new(addresses);
    loop {
        info!("Connecting to Iggy cluster...");
        if cluster.init().await.is_ok() {
            info!("Connected to Iggy cluster.");
            break;
        }

        error!("Cannot connect to Iggy cluster, reconnecting in {reconnection_interval} ms...");
        sleep(Duration::from_millis(reconnection_interval)).await;
        continue;
    }

    let stdin = io::stdin();
    let mut user_input = String::new();
    loop {
        info!("Enter command to send to the cluster: ");
        user_input.clear();
        stdin.read_line(&mut user_input)?;
        if user_input.contains('\n') {
            user_input.pop();
        }
        if user_input.contains('\r') {
            user_input.pop();
        }

        let command = command_parser::parse(&user_input);
        if command.is_none() {
            error!("Invalid command: {}", user_input);
            continue;
        }

        let command = command.unwrap();
        if cluster.send(&command).await.is_err() {
            error!("There was an error sending the command to the cluster.");
        }
    }
}
