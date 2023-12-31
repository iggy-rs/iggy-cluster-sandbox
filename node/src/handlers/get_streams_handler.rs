use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::bytes_serializable::BytesSerializable;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    let streams = cluster.get_streams().await?;
    let mut bytes: Vec<u8> = Vec::new();
    for stream in streams {
        bytes.extend(&stream.as_bytes());
    }
    handler.send_ok_response(&bytes).await?;
    Ok(())
}
