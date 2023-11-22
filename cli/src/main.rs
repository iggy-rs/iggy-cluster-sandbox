use figlet_rs::FIGfont;
use monoio::io::AsyncWriteRentExt;
use monoio::net::TcpStream;
use monoio::time::sleep;
use monoio::utils::CtrlC;
use sdk::commands::ping::Ping;
use sdk::error::SystemError;
use std::env;
use std::time::Duration;
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
    info!("Sending ping command to Iggy node...");
    let ping = Ping::new_command();
    let (result, _) = tcp_stream.write_all(ping.as_bytes()).await;
    if result.is_err() {
        error!("Failed to send ping command to Iggy node.");
        return Err(SystemError::CannotConnectToClusterNode(address.to_string()));
    }
    info!("Received pong response from Iggy node.");
    info!("Press CTRL+C shutdown Iggy CLI...");
    CtrlC::new().unwrap().await;
    info!("Iggy CLI has shutdown successfully.");
    Ok(())
}
