use crate::clusters::cluster::Cluster;
use crate::clusters::nodes::node::Node;
use crate::types::NodeId;
use sdk::error::SystemError;
use std::collections::HashMap;
use tracing::{error, info, warn};

impl Cluster {
    pub async fn sync_nodes_state(&self) -> Result<Vec<NodeId>, SystemError> {
        let mut states = HashMap::new();
        let self_node = self.get_self_node();
        if self_node.is_none() {
            error!("Failed to get self node.");
            return Err(SystemError::UnhealthyCluster);
        }

        info!("Syncing state from cluster nodes...");
        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            let state = node.node.get_node_state().await;
            if state.is_err() {
                let error = state.unwrap_err();
                error!(
                    "Failed to sync state from cluster node with ID: {}, {error}",
                    node.node.id
                );
                continue;
            }

            let state = state.unwrap();
            states.insert(node.node.id, state);
        }
        info!("Synced state from cluster nodes.");

        let mut nodes_by_last_applied = HashMap::new();
        for (node_id, state) in states.iter() {
            let last_applied = state.last_applied;
            let nodes = nodes_by_last_applied
                .entry(last_applied)
                .or_insert(Vec::new());
            nodes.push(node_id);
        }

        let mut available_leaders = Vec::new();
        let self_node = self_node.unwrap();
        if nodes_by_last_applied.is_empty() {
            info!("This node cannot be a leader because there are no other nodes in the cluster.");
            set_leader_unavailable(&self_node.node).await;
            return Ok(available_leaders);
        }

        if states.iter().all(|(_, state)| state.last_applied == 0) {
            info!("This node can be a leader because it's state is initial state of the cluster.");
            set_leader_available(&self_node.node).await;
            for node in self.nodes.values() {
                available_leaders.push(node.node.id);
            }
            return Ok(available_leaders);
        }

        let min_last_applied = *nodes_by_last_applied.keys().min().unwrap();
        let self_state = self.get_node_state().await.unwrap();
        for (_, nodes) in nodes_by_last_applied
            .iter()
            .filter(|(last_applied, _)| *last_applied >= &min_last_applied)
        {
            for node_id in nodes {
                available_leaders.push(**node_id);
            }
        }

        if self_state.last_applied < min_last_applied {
            set_leader_unavailable(&self_node.node).await;
            warn!("This node cannot be a leader because it's state is behind other nodes, last applied: {} < {}", self_state.last_applied, min_last_applied);
            return Ok(available_leaders);
        }

        let nodes_with_lower_last_applied = nodes_by_last_applied
            .iter()
            .filter(|(last_applied, _)| *last_applied < &self_state.last_applied)
            .map(|(_, nodes)| nodes.len() as u64)
            .sum::<u64>();

        let nodes_with_same_last_applied = 1 + nodes_by_last_applied
            .get(&self_state.last_applied)
            .unwrap_or(&Vec::new())
            .len() as u64;

        let quorum = self.get_quorum_count();
        if nodes_with_lower_last_applied + nodes_with_same_last_applied < quorum {
            set_leader_unavailable(&self_node.node).await;
            warn!("This node cannot be a leader because it's state is behind other nodes, last applied: {} < {}", self_state.last_applied, min_last_applied);
            return Ok(available_leaders);
        }

        let all_last_applied_states = states
            .iter()
            .map(|(node_id, state)| format!("{} -> {}", node_id, state.last_applied))
            .collect::<Vec<String>>()
            .join(", ");
        set_leader_available(&self_node.node).await;
        info!("This node can be a leader because it's state: {} is up to date with other nodes, all last applied states by nodes: {all_last_applied_states}", self_state.last_applied);
        Ok(available_leaders)
    }
}

async fn set_leader_unavailable(node: &Node) {
    node.set_can_be_leader(false).await;
}

async fn set_leader_available(node: &Node) {
    node.set_can_be_leader(true).await;
}
