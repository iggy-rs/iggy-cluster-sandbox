use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use crate::server::tcp_listener::listen;
use monoio::net::TcpListener;
use sdk::error::SystemError;
use std::io::ErrorKind;
use std::rc::Rc;
use tracing::{error, info};

pub fn start(name: &str, address: &str, cluster: Rc<Cluster>) {
    info!("Initializing {name} on TCP address: {address}...");
    let address = address.to_string();
    let node_address = address.clone();
    let server_name = name.to_string();
    monoio::spawn(async move {
        let listener = TcpListener::bind(address.clone());
        if listener.is_err() {
            panic!("Unable to start {server_name} on TCP address: {address}.");
        }

        let listener = listener.unwrap();
        loop {
            let cluster = cluster.clone();
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("{server_name} has accepted new TCP connection: {address}");
                    let mut connection = ConnectionHandler::new(stream, 0);
                    monoio::spawn(async move {
                        if let Err(error) = listen(&mut connection, cluster).await {
                            handle_error(error);
                        }
                    });
                }
                Err(error) => {
                    error!("{server_name} is unable to accept TCP connection, error: {error}")
                }
            }
        }
    });
    info!("{name} has started on TCP address: {node_address}");
}

fn handle_error(error: SystemError) {
    match error {
        SystemError::IoError(error) => match error.kind() {
            ErrorKind::UnexpectedEof => {
                info!("Connection has been closed.");
            }
            ErrorKind::ConnectionAborted => {
                info!("Connection has been aborted.");
            }
            ErrorKind::ConnectionRefused => {
                info!("Connection has been refused.");
            }
            ErrorKind::ConnectionReset => {
                info!("Connection has been reset.");
            }
            _ => {
                error!("Connection has failed: {error}");
            }
        },
        _ => {
            error!("Connection has failed: {error}");
        }
    }
}
