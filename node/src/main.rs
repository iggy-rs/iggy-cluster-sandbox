use crate::clusters::cluster::Cluster;
use crate::configs::config_provider::FileConfigProvider;
use crate::error::SystemError;
use crate::server::tcp_server;
use figlet_rs::FIGfont;
use monoio::utils::CtrlC;
use tracing::info;

mod clusters;
mod command;
mod configs;
mod error;
mod server;

#[monoio::main(timer_enabled = true)]
async fn main() -> Result<(), SystemError> {
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Node");
    let config_provider = FileConfigProvider::default();
    println!("{}", figure.unwrap());
    let system_config = config_provider.load_config().await?;
    println!("{system_config}");
    tcp_server::start(&system_config.node.address);

    let mut cluster = Cluster::new(&system_config.node.name, &system_config.node.address)?;
    for node in system_config.cluster.nodes {
        cluster.add_node(&node.name, &node.address)?;
    }
    cluster.connect().await?;
    cluster.start_healthcheck()?;
    info!("Press CTRL+C shutdown Iggy node...");
    CtrlC::new().unwrap().await;
    cluster.disconnect().await?;
    info!("Iggy node has shutdown successfully.");

    Ok(())
}
