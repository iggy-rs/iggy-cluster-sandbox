use crate::clusters::cluster::{Cluster, ClusterNodeState};
use crate::clusters::election::ElectionState;
use crate::types::{CandidateId, NodeId, TermId};
use sdk::error::SystemError;
use tracing::info;

impl Cluster {
    pub async fn start_election(&self) -> Result<(), SystemError> {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        let self_node = self_node.unwrap();
        self_node.set_state(ClusterNodeState::Candidate).await;
        let term = self.election_manager.next_term().await;
        let election_state = self.election_manager.start_election(term).await;
        match election_state {
            ElectionState::InProgress => {
                info!("Election for term: {term} is in progress...");
            }
            ElectionState::LeaderElected(leader_id) => {
                info!("Election for term: {term} has completed, leader ID: {leader_id}.");
                if leader_id == self_node.node.id {
                    self_node.set_state(ClusterNodeState::Leader).await;
                    info!("Your role is leader, term: {term}.");
                } else {
                    self_node.set_state(ClusterNodeState::Follower).await;
                    info!("Your role is follower, term: {term}.");
                }
            }
            ElectionState::NoLeaderElected => {
                info!("Election for term: {term} has completed, no leader elected, requesting votes...");
                self.request_votes(term).await?;
            }
        }

        Ok(())
    }

    pub async fn request_votes(&self, term: TermId) -> Result<(), SystemError> {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        if self.election_manager.is_election_completed().await {
            info!("Election is over, ignoring vote requests.");
            return Ok(());
        }

        info!("Voting for yourself, term: {term}...");
        let self_node = self_node.unwrap();
        let node_id = self_node.node.id;
        self.vote(term, node_id, node_id).await?;
        // TODO: Request votes from the other nodes.

        Ok(())
    }

    pub async fn vote(
        &self,
        term: TermId,
        candidate_id: CandidateId,
        node_id: NodeId,
    ) -> Result<(), SystemError> {
        let self_node = self.get_self_node();
        if self_node.is_none() {
            return Err(SystemError::UnhealthyCluster);
        }

        self.election_manager
            .vote(term, candidate_id, node_id)
            .await;

        Ok(())
    }
}
