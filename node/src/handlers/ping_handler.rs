use crate::connection::tcp_connection::TcpConnection;
use sdk::error::SystemError;
use tracing::info;

pub(crate) async fn handle(handler: &mut TcpConnection) -> Result<(), SystemError> {
    info!("Received a ping command.");
    handler.send_empty_ok_response().await?;
    info!("Sent a ping response.");
    Ok(())
}
