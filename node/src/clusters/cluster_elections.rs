use crate::clusters::cluster::{Cluster, ClusterNodeState};
use crate::clusters::election::ElectionState;
use crate::types::{CandidateId, NodeId, TermId};
use sdk::error::SystemError;
use tracing::{info, warn};

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
            let election_state = self.election_manager.start_election(term).await;
            match election_state {
                ElectionState::LeaderElected(leader_id) => {
                    info!("Election for term: {term} has completed, leader ID: {leader_id}.");
                    if leader_id == self_node.node.id {
                        self_node.set_state(ClusterNodeState::Leader).await;
                        info!("Your role is leader, term: {term}.");
                    } else {
                        self_node.set_state(ClusterNodeState::Follower).await;
                        info!("Your role is follower, term: {term}.");
                    }
                    break;
                }
                ElectionState::NoLeaderElected => {
                    info!("Election for term: {term} has completed, no leader elected, requesting votes...");
                    if self.request_votes(term).await.is_err() {
                        info!("Requesting votes failed.");
                        continue;
                    }

                    info!("Updating leader to your node, term: {term}...");
                    if self.update_leader(term).await.is_err() {
                        info!("Updating leader failed.");
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn update_leader(&self, term: TermId) -> Result<(), SystemError> {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        for node in &self.nodes {
            if node.node.is_self {
                continue;
            }

            info!(
                "Updating leader, sending request to node: {}...",
                node.node.id
            );
            if let Err(err) = node.node.update_leader(term).await {
                info!(
                    "Update leader request failed in node:{} with: {err}.",
                    node.node.id
                );
                continue;
            }
            info!("Update leader request sent to node: {}.", node.node.id);
        }

        Ok(())
    }

    pub async fn request_votes(&self, term: TermId) -> Result<(), SystemError> {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        info!("Voting for yourself, term: {term}...");
        let self_node_id = self_node.unwrap().node.id;
        self.vote(term, self_node_id, self_node_id).await?;
        for node in &self.nodes {
            if node.node.is_self {
                continue;
            }
            info!("Requesting vote from node: {}...", node.node.id);
            if let Err(err) = node.node.request_vote(term).await {
                info!("Vote request failed: {err}.");
                continue;
            }

            info!("Requested vote from node: {}.", node.node.id);
            self.vote(term, self_node_id, node.node.id).await?;
        }

        Ok(())
    }

    pub async fn vote(
        &self,
        term: TermId,
        candidate_id: CandidateId,
        node_id: NodeId,
    ) -> Result<(), SystemError> {
        let voted = self
            .election_manager
            .vote(term, candidate_id, node_id)
            .await;

        if voted {
            info!("Vote accepted.");
            Ok(())
        } else {
            warn!("Vote rejected.");
            Err(SystemError::VoteRejected)
        }
    }
}
