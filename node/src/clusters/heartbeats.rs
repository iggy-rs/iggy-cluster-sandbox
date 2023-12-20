use crate::clusters::cluster::Cluster;
use monoio::time::sleep;
use sdk::timestamp::TimeStamp;
use std::rc::Rc;
use tracing::{error, info, warn};

pub fn subscribe(cluster: Rc<Cluster>) {
    monoio::spawn(async move {
        listen(cluster).await;
    });
}

async fn listen(cluster: Rc<Cluster>) {
    let interval = cluster.heartbeat_interval;
    let interval_ms = interval.as_millis() as u64;
    let interval_micros = interval.as_micros() as u64;
    loop {
        sleep(interval).await;
        let last_heartbeat = cluster.election_manager.get_last_heartbeat().await;
        if last_heartbeat == 0 {
            continue;
        }

        let leader_id = cluster.election_manager.get_leader_id().await;
        if leader_id.is_none() {
            continue;
        }

        let leader_id = leader_id.unwrap();
        if leader_id == cluster.get_self_node().unwrap().node.id {
            continue;
        }

        let now = TimeStamp::now().to_micros();
        if now - last_heartbeat <= interval_micros {
            info!(
                "Received a heartbeat from cluster node ID: {leader_id} in desired interval: {interval_ms} ms."
            );
            continue;
        }

        warn!(
            "Failed to receive a heartbeat from cluster node ID: {leader_id} in desired interval: {interval_ms} ms.",
        );

        cluster.election_manager.remove_leader().await;
        if let Err(error) = cluster.start_election().await {
            error!("Failed to start election: {}", error);
        }
    }
}
