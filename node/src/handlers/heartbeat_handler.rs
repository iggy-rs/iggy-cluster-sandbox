use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use sdk::commands::heartbeat::Heartbeat;
use sdk::error::SystemError;
use sdk::timestamp::TimeStamp;
use std::rc::Rc;

pub(crate) async fn handle(
    handler: &mut ConnectionHandler,
    command: &Heartbeat,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
    cluster.election_manager.set_term(command.term).await;
    if let Some(leader_id) = command.leader_id {
        cluster.set_leader(command.term, leader_id).await;
    }

    if let Some(leader_id) = cluster.election_manager.get_leader_id().await {
        if leader_id != cluster.get_self_node().unwrap().node.id {
            cluster
                .election_manager
                .set_last_heartbeat(TimeStamp::now().to_micros())
                .await;
        }
    }

    handler.send_empty_ok_response().await?;
    Ok(())
}
