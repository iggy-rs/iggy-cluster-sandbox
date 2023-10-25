use crate::client::cluster_node_client::ClusterNodeClient;
use crate::configs::config_provider::FileConfigProvider;
use crate::error::SystemError;
use crate::server::tcp_server;
use figlet_rs::FIGfont;
use tracing::info;

mod client;
mod configs;
mod error;
mod server;

#[tokio::main]
async fn main() -> Result<(), SystemError> {
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Node");
    let config_provider = FileConfigProvider::default();
    println!("{}", figure.unwrap());
    let system_config = config_provider.load_config().await?;
    println!("{system_config}");
    tcp_server::start(&system_config.node.address);
    let mut cluster_node_clients = Vec::new();

    for cluster_member in system_config.cluster.members {
        let cluster_node_client = ClusterNodeClient::new_with_defaults(&cluster_member.address)?;
        cluster_node_client.connect().await?;
        cluster_node_clients.push(cluster_node_client);
    }

    #[cfg(unix)]
    let (mut ctrl_c, mut sigterm) = {
        use tokio::signal::unix::{signal, SignalKind};
        (
            signal(SignalKind::interrupt())?,
            signal(SignalKind::terminate())?,
        )
    };

    #[cfg(windows)]
    let mut ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    tokio::select! {
        _ = ctrl_c.recv() => {
            info!("Received SIGINT. Shutting down Iggy node...");
        },
        _ = sigterm.recv() => {
            info!("Received SIGTERM. Shutting down Iggy node...");
        }
    }

    #[cfg(windows)]
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Received CTRL-C. Shutting down Iggy node...");
        }
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        }
    }

    for cluster_node_client in cluster_node_clients {
        cluster_node_client.disconnect().await?;
    }

    info!("Iggy node has shutdown successfully.");

    Ok(())
}
