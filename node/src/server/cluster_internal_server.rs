use crate::clusters::cluster::Cluster;
use crate::connection::tcp_handler::TcpHandler;
use crate::server::tcp_connection::{handle_error, tcp_listener};
use monoio::net::TcpListener;
use std::rc::Rc;
use tracing::{error, info};

pub fn start(address: &str, cluster: Rc<Cluster>) {
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
            let cluster = cluster.clone();
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("Accepted new TCP connection: {address}");
                    let mut handler = TcpHandler::new(stream);
                    monoio::spawn(async move {
                        if let Err(error) = tcp_listener(&mut handler, cluster).await {
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
