use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::bytes_serializable::BytesSerializable;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    let state = cluster.get_node_state().await?;
    handler.send_ok_response(&state.as_bytes()).await?;
    Ok(())
}
