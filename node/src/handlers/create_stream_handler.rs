use crate::clusters::cluster::Cluster;
use crate::connection::tcp_connection::TcpConnection;
use sdk::commands::create_stream::CreateStream;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut TcpConnection,
    _command: &CreateStream,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    handler.send_empty_ok_response().await?;
    Ok(())
}
