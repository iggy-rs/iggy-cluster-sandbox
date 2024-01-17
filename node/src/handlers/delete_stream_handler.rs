use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::delete_stream::DeleteStream;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &DeleteStream,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    cluster.verify_is_leader().await?;
    let term = cluster.election_manager.get_current_term().await;
    cluster.delete_stream(term, command.id).await?;
    handler.send_empty_ok_response().await
}
