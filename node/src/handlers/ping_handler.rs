use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    handler.send_empty_ok_response().await?;
    Ok(())
}
