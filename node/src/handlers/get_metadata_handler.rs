use crate::clusters::cluster::Cluster;
use crate::connection::tcp_connection::TcpConnection;
use sdk::bytes_serializable::BytesSerializable;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut TcpConnection,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    let metadata = cluster.get_metadata().await;
    handler.send_ok_response(&metadata.as_bytes()).await?;
    Ok(())
}
