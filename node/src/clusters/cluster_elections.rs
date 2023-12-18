use crate::clusters::cluster::{Cluster, ClusterNodeState};
use crate::clusters::election::ElectionState;
use crate::types::{CandidateId, NodeId, TermId};
use sdk::error::SystemError;
use tracing::{error, info, warn};

impl Cluster {
    pub async fn start_election(&self) -> Result<(), SystemError> {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        let self_node = self_node.unwrap();
        loop {
            self_node.set_state(ClusterNodeState::Candidate).await;
            let term = self.election_manager.next_term().await;
            info!("Set term: {term}.");
            let election_state = self.election_manager.start_election(term).await;
            match election_state {
                ElectionState::TermChanged(new_term) => {
                    if let Some(leader) = self.election_manager.get_leader_id().await {
                        info!("Leader ID: {leader} was elected in new term: {new_term}.");
                        break;
                    }
                    warn!("No leader elected in term: {new_term}.");
                    continue;
                }
                ElectionState::LeaderElected(leader_id) => {
                    let term = self.election_manager.get_current_term().await;
                    info!("Election in term: {term} has completed, leader ID: {leader_id}.");
                    if leader_id == self_node.node.id {
                        self_node.set_state(ClusterNodeState::Leader).await;
                        info!("Your role is leader, term: {term}.");
                        self.start_heartbeat()?;
                    } else {
                        self_node.set_state(ClusterNodeState::Follower).await;
                        info!("Your role is follower, term: {term}.");
                    }
                    break;
                }
                ElectionState::NoLeaderElected => {
                    let term = self.election_manager.get_current_term().await;
                    info!("Election in term: {term} has completed, no leader elected, requesting votes...");
                    if self.request_votes(term).await.is_err() {
                        warn!("Requesting votes failed.");
                        self.election_manager.remove_leader().await;
                        continue;
                    }

                    if !self.has_majority_votes(term).await {
                        warn!("No majority votes.");
                        self.election_manager.remove_leader().await;
                        continue;
                    }

                    info!("Updating leader to your node, term: {term}...");
                    if self.update_leader(term).await.is_err() {
                        warn!("Updating leader failed.");
                        self.election_manager.remove_leader().await;
                        continue;
                    }

                    info!("Election in term: {term} has completed, this node is a leader with ID: {}.", self_node.node.id);
                    self.start_heartbeat()?;
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn set_leader(&self, term: TermId, leader_id: NodeId) -> Result<(), SystemError> {
        if self
            .election_manager
            .set_leader(term, leader_id)
            .await
            .is_err()
        {
            warn!("Setting leader failed.");
            return Ok(());
        }

        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            node.node.set_leader(term, leader_id).await;
        }

        Ok(())
    }

    pub async fn update_leader(&self, term: TermId) -> Result<(), SystemError> {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        let leader_id = self_node.unwrap().node.id;
        let mut updated_nodes_count = 1;
        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }

            info!(
                "Updating leader, sending request to node ID: {}...",
                node.node.id
            );
            if let Err(err) = node.node.update_leader(term, leader_id).await {
                error!(
                    "Update leader request failed in node ID: {} with: {err}.",
                    node.node.id
                );
                continue;
            }
            updated_nodes_count += 1;
            node.node.set_leader(term, leader_id).await;
            info!("Update leader request sent to node ID: {}.", node.node.id);
        }

        if updated_nodes_count < self.election_manager.get_required_votes_count() {
            return Err(SystemError::LeaderRejected);
        }

        Ok(())
    }

    pub async fn request_votes(&self, term: TermId) -> Result<(), SystemError> {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        info!("Voting for yourself in term: {term}...");
        let self_node_id = self_node.unwrap().node.id;
        self.vote(term, self_node_id, self_node_id).await?;
        let mut votes_count = 1;
        for node in self.nodes.values() {
            if node.node.is_self_node() {
                continue;
            }
            info!(
                "Requesting vote from node: {} in term: {term}...",
                node.node.id
            );
            if let Err(err) = node.node.request_vote(term).await {
                match err {
                    SystemError::InvalidResponse(status, payload) => {
                        error!(
                            "Vote request from node: {} in term: {term} failed, status: {status}.",
                            node.node.id
                        );
                        if status == 26 {
                            let payload = payload.unwrap();
                            let new_term = u64::from_le_bytes(payload.try_into().unwrap());
                            if new_term > term {
                                error!("Invalid current term: {term}, new term: {new_term}");
                                self.election_manager.set_term(new_term).await;
                                continue;
                            }
                        } else {
                            error!(
                                "Invalid response from node: {node_id}.",
                                node_id = node.node.id
                            );
                            continue;
                        }
                    }
                    _ => {
                        error!(
                            "Vote request from node: {} in term: {term} failed, error: {err}.",
                            node.node.id
                        );
                        continue;
                    }
                }
            }

            votes_count += 1;
            info!(
                "Successfully requested vote from node: {} in term: {term}.",
                node.node.id
            );
            self.vote(term, self_node_id, node.node.id).await?;
        }

        info!("Managed to request votes from {votes_count} nodes in term: {term}.");
        Ok(())
    }

    pub async fn has_majority_votes(&self, term: TermId) -> bool {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return false;
        }

        let self_node_id = self_node.unwrap().node.id;
        self.election_manager
            .has_majority_votes(term, self_node_id)
            .await
    }

    pub async fn vote(
        &self,
        term: TermId,
        candidate_id: CandidateId,
        node_id: NodeId,
    ) -> Result<(), SystemError> {
        self.election_manager
            .vote(term, candidate_id, node_id)
            .await
    }
}
