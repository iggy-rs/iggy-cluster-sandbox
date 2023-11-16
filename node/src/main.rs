use crate::clusters::cluster::Cluster;
use crate::configs::config_provider::FileConfigProvider;
use crate::error::SystemError;
use crate::server::tcp_server;
use crate::streaming::data_appender::DataAppender;
use figlet_rs::FIGfont;
use monoio::utils::CtrlC;
use std::rc::Rc;
use tracing::info;

mod bytes_serializable;
mod clusters;
mod commands;
mod configs;
mod connection;
mod error;
mod server;
mod streaming;

#[monoio::main(timer_enabled = true)]
async fn main() -> Result<(), SystemError> {
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Node");
    let config_provider = FileConfigProvider::default();
    println!("{}", figure.unwrap());
    let system_config = config_provider.load_config().await?;
    println!("{system_config}");
    let data_appender = DataAppender::new(&system_config.stream.path);
    info!("{data_appender}");
    let cluster = Cluster::new(
        &system_config.node.name,
        &system_config.node.address,
        &system_config.cluster,
    )?;
    let cluster = Rc::new(cluster);
    tcp_server::start(&system_config.node.address, cluster.clone());
    cluster.connect().await?;
    cluster.start_healthcheck()?;
    info!("Press CTRL+C shutdown Iggy node...");
    CtrlC::new().unwrap().await;
    cluster.disconnect().await?;
    info!("Iggy node has shutdown successfully.");
    Ok(())
}
