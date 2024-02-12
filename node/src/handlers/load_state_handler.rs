use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::load_state::LoadState;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    _command: &LoadState,
    _cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    // TODO: Load the state from the cluster
    handler.send_empty_ok_response().await?;
    Ok(())
}
