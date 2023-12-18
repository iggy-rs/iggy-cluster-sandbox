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
                    let mut connection = ConnectionHandler::new(stream, address, 0);
                    monoio::spawn(async move {
                        let cluster_error = cluster.clone();
                        if let Err(error) = listen(&mut connection, cluster).await {
                            handle_error(error, &connection);
                            cluster_error
                                .handle_disconnected_node(connection.node_id)
                                .await;
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

fn handle_error(error: SystemError, connection_handler: &ConnectionHandler) {
    match error {
        SystemError::IoError(error) => match error.kind() {
            ErrorKind::UnexpectedEof => {
                info!(
                    "Connection has been closed by node ID: {} with address: {}.",
                    connection_handler.node_id, connection_handler.address
                );
            }
            ErrorKind::ConnectionAborted => {
                info!(
                    "Connection has been aborted by node ID: {} with address: {}.",
                    connection_handler.node_id, connection_handler.address
                );
            }
            ErrorKind::ConnectionRefused => {
                info!(
                    "Connection has been refused by node ID: {} with address: {}.",
                    connection_handler.node_id, connection_handler.address
                );
            }
            ErrorKind::ConnectionReset => {
                info!(
                    "Connection has been reset by node ID: {} with address: {}.",
                    connection_handler.node_id, connection_handler.address
                );
            }
            _ => {
                error!(
                    "Connection has failed by node ID: {} with address: {}. Error: {error}",
                    connection_handler.node_id, connection_handler.address
                );
            }
        },
        _ => {
            error!(
                "Connection has failed by node ID: {} with address: {}. Error: {error}",
                connection_handler.node_id, connection_handler.address
            );
        }
    }
}
