mod command_handler;
mod command_parser;

use figlet_rs::FIGfont;
use sdk::clients::cluster_client::ClusterClient;
use sdk::error::SystemError;
use std::{env, io};
use tracing::{error, info};

#[monoio::main(timer_enabled = true)]
async fn main() -> Result<(), SystemError> {
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy CLI");
    println!("{}", figure.unwrap());
    let reconnection_interval = 1000;
    let reconnection_retries = 10;
    let addresses = env::var("IGGY_CLUSTER_ADDRESS")
        .unwrap_or("127.0.0.1:8101,127.0.0.1:8102,127.0.0.1:8103".to_string());
    let addresses: Vec<&str> = addresses.split(',').collect();
    let mut cluster = ClusterClient::new(addresses, reconnection_interval, reconnection_retries);
    cluster.init().await?;
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
        command_handler::handle(&command, &cluster).await;
    }
}
