use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::create_stream::CreateStream;
use sdk::error::SystemError;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &CreateStream,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.verify_is_healthy().await?;
    cluster.verify_is_leader().await?;
    let term = cluster.election_manager.get_current_term().await;
    cluster
        .create_stream(
            Some(term),
            command.id,
            command.replication_factor.unwrap_or(3),
        )
        .await?;
    cluster
        .sync_created_stream(handler, term, command.id, command.replication_factor)
        .await
}
