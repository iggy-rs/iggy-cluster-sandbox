mod client;
mod command_parser;

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
    let address = env::var("IGGY_NODE_ADDRESS").unwrap_or("127.0.0.1:8101".to_string());
    let mut client;
    loop {
        info!("Connecting to Iggy node: {address}...");
        if let Ok(connected_client) = client::Client::init(&address).await {
            client = connected_client;
            info!("Connected to Iggy node: {address}.");
            break;
        }

        error!("Cannot connect to Iggy node, reconnecting in {reconnection_interval} ms...");
        sleep(Duration::from_millis(reconnection_interval)).await;
        continue;
    }

    let stdin = io::stdin();
    let mut user_input = String::new();
    loop {
        info!("Enter command to send to the node: ");
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
        if client.send(command).await.is_err() {
            error!("There was an error sending the command to the node.");
        }
    }
}
