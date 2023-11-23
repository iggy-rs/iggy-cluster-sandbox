mod command_parser;

use figlet_rs::FIGfont;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
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
    let address = env::var("IGGY_NODE_ADDRESS").unwrap_or("127.0.0.1:8100".to_string());
    let mut tcp_stream;
    loop {
        info!("Connecting to Iggy node: {address}...");
        if let Ok(connection) = TcpStream::connect(&address).await {
            tcp_stream = connection;
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
        info!("Sending command to Iggy node...");
        let (result, _) = tcp_stream.write_all(command.as_bytes()).await;
        if result.is_err() {
            error!("Failed to send command to Iggy node.");
            continue;
        }
        info!("Command sent to Iggy node.");

        let buffer = vec![0u8; 8];
        let (read_bytes, buffer) = tcp_stream.read(buffer).await;
        if read_bytes.is_err() {
            error!("Failed to read a response: {:?}", read_bytes.err());
            continue;
        }

        let status = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        let payload_length = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        if status == 0 {
            info!("Received OK response from Iggy node, payload length: {payload_length}.",);
            continue;
        }
        error!("Received error response from Iggy node, status: {status}.",);
    }
}
