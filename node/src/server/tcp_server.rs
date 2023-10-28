use crate::server::tcp_handler::{handle_connection, handle_error};
use crate::server::tcp_sender::TcpSender;
use monoio::net::TcpListener;
use tracing::{error, info};

pub fn start(address: &str) {
    info!("Initializing Iggy node on TCP address: {address}...");
    let address = address.to_string();
    let node_address = address.clone();
    monoio::spawn(async move {
        let listener = TcpListener::bind(address.clone());
        if listener.is_err() {
            panic!("Unable to start node on TCP address: {address}.");
        }

        let listener = listener.unwrap();
        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("Accepted new TCP connection: {address}");
                    let mut sender = TcpSender { stream };
                    monoio::spawn(async move {
                        if let Err(error) = handle_connection(&mut sender).await {
                            handle_error(error);
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP connection, error: {error}"),
            }
        }
    });
    info!("Iggy node has started on TCP address: {node_address}");
}
