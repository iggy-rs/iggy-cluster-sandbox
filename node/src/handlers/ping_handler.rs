use crate::connection::handler::ConnectionHandler;
use sdk::error::SystemError;

pub(crate) async fn handle(handler: &mut ConnectionHandler) -> Result<(), SystemError> {
    handler.send_empty_ok_response().await?;
    Ok(())
}
