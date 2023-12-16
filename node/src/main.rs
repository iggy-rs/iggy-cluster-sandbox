use crate::clusters::cluster::{Cluster, SelfNode};
use crate::configs::config_provider::FileConfigProvider;
use crate::server::{public_server, sync_server};
use crate::streaming::streamer::Streamer;
use figlet_rs::FIGfont;
use monoio::utils::CtrlC;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::info;

mod clusters;
mod configs;
mod connection;
mod handlers;
mod server;
mod streaming;
mod types;

#[monoio::main(timer_enabled = true)]
async fn main() -> Result<(), SystemError> {
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Node");
    println!("{}", figure.unwrap());
    let config_provider = FileConfigProvider::default();
    let system_config = config_provider.load_config().await?;
    println!("{system_config}");
    let mut streamer = Streamer::new(system_config.node.id, &system_config.stream.path);
    streamer.init().await;
    let cluster = Cluster::new(
        SelfNode::new(
            system_config.node.id,
            &system_config.node.name,
            &system_config.node.address,
        ),
        &system_config.cluster,
        streamer,
    )?;
    let cluster = Rc::new(cluster);
    sync_server::start(&system_config.node.address, cluster.clone());
    public_server::start(&system_config.server.address, cluster.clone());
    cluster.connect().await?;
    cluster.start_heartbeat()?;
    cluster.start_election().await?;
    info!("Press CTRL+C shutdown Iggy node...");
    CtrlC::new().unwrap().await;
    cluster.disconnect().await?;
    info!("Iggy node has shutdown successfully.");
    Ok(())
}
