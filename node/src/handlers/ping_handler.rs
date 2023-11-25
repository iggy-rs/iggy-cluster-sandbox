use crate::connection::tcp_handler::TcpHandler;
use sdk::error::SystemError;
use tracing::info;

pub async fn handle(handler: &mut TcpHandler) -> Result<(), SystemError> {
    info!("Received a ping command.");
    handler.send_empty_ok_response().await?;
    info!("Sent a ping response.");
    Ok(())
}
