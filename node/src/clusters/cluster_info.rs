use crate::clusters::cluster::Cluster;
use monoio::time::sleep;
use std::rc::Rc;
use tracing::info;

const SEPARATOR: &str = "------------------------------------------";

pub fn subscribe(cluster: Rc<Cluster>) {
    monoio::spawn(async move {
        listen(cluster).await;
    });
}

async fn listen(cluster: Rc<Cluster>) {
    let interval = cluster.info_interval;
    loop {
        sleep(interval).await;
        let mut info = String::new();
        let health = cluster.get_health().await;
        info.push_str(&format!("\n\n{SEPARATOR}\n"));
        info.push_str(&format!("+++ Cluster is {health}\n"));
        let state = cluster.state.lock().await;
        info.push_str(&format!("+++ Nodes: {}\n", cluster.nodes.len()));
        info.push_str(&format!("+++ Term: {}\n", state.term));
        info.push_str(&format!("+++ Commit index: {}\n", state.commit_index));
        info.push_str(&format!("+++ Last applied: {}\n", state.last_applied));
        info.push_str(&format!("{SEPARATOR}\n"));
        info!("{info}");
    }
}
