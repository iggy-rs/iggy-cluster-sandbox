use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::bytes_serializable::BytesSerializable;
use sdk::commands::load_state::LoadState;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &LoadState,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    let appended_state = cluster.load_appended_state(command.start_index).await?;
    handler.send_ok_response(&appended_state.as_bytes()).await?;
    Ok(())
}
