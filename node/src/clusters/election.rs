use crate::types::{CandidateId, NodeId, TermId};
use futures::lock::Mutex;
use monoio::time::sleep;
use rand::rngs::ThreadRng;
use rand::Rng;
use sdk::error::SystemError;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug)]
pub(crate) struct Election {
    pub is_completed: Mutex<bool>,
    pub term: Mutex<TermId>,
    pub leader_id: Mutex<Option<NodeId>>,
    pub votes_count: Mutex<HashMap<CandidateId, HashSet<NodeId>>>,
    pub voted_for: Mutex<Option<CandidateId>>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ElectionState {
    LeaderElected(u64),
    NoLeaderElected,
}

impl Default for Election {
    fn default() -> Self {
        Self {
            is_completed: Mutex::new(true),
            term: Mutex::new(0),
            leader_id: Mutex::new(None),
            votes_count: Mutex::new(HashMap::new()),
            voted_for: Mutex::new(None),
        }
    }
}

#[derive(Debug)]
pub struct ElectionManager {
    self_id: CandidateId,
    current_term: Mutex<TermId>,
    current_leader_id: Mutex<Option<CandidateId>>,
    election: Election,
    randomizer: Mutex<ThreadRng>,
    required_votes_count: usize,
}

impl ElectionManager {
    pub fn new(self_id: CandidateId, required_votes_count: usize) -> Self {
        Self {
            self_id,
            current_term: Mutex::new(0),
            current_leader_id: Mutex::new(None),
            election: Election::default(),
            randomizer: Mutex::new(rand::thread_rng()),
            required_votes_count,
        }
    }

    pub async fn set_leader(
        &self,
        term: TermId,
        leader_id: CandidateId,
    ) -> Result<(), SystemError> {
        let mut current_term = self.current_term.lock().await;
        if term < *current_term {
            let this_term = *current_term;
            *current_term = term;
            warn!(
                "Received leader ID: {leader_id} for term: {term}, but current term is: {this_term}.",
            );
            return Err(SystemError::LeaderRejected);
        }

        let this_term = *current_term;
        info!("Received leader ID: {leader_id} for term: {term}, current term: {this_term}.");
        *current_term = term;
        *self.current_leader_id.lock().await = Some(leader_id);
        Ok(())
    }

    pub async fn next_term(&self) -> TermId {
        let current_term = self.current_term.lock().await;
        *current_term + 1
    }

    pub async fn start_election(&self, term: TermId) -> ElectionState {
        self.set_election_completed_state(false).await;
        *self.election.term.lock().await = term;
        self.election.voted_for.lock().await.take();
        self.election.votes_count.lock().await.clear();
        let previous_term;

        {
            let mut current_term = self.current_term.lock().await;
            previous_term = *current_term;
            *current_term = term;
        }

        let timeout = self.randomizer.lock().await.gen_range(150..=300);
        info!("Starting election for new term {term}, previous term: {previous_term}, required votes: {} timeout: {timeout} ms...", self.required_votes_count);
        // Wait for random timeout and check if there is no leader in the meantime
        sleep(Duration::from_millis(timeout)).await;
        self.count_votes(term).await
    }

    pub async fn vote(&self, term_id: TermId, candidate_id: CandidateId, node_id: NodeId) -> bool {
        info!("Trying to vote for term: {term_id}, candidate ID: {candidate_id} from node ID: {node_id}.");
        if self.is_election_completed().await {
            warn!("Election is over, ignoring vote request.");
            return false;
        }

        let current_term = *self.current_term.lock().await;
        if term_id < current_term {
            warn!(
                "Received vote request for term: {term_id}, candidate ID: {candidate_id} from node ID: {node_id}, but current term is: {current_term}.",
            );
            return false;
        }

        info!(
            "Received vote request for term: {term_id}, current term: {current_term}, candidate ID: {candidate_id} from node ID: {node_id}.",
        );
        if term_id > current_term {
            info!("Updating current term to: {term_id} and resetting previous votes...");
            *self.current_term.lock().await = term_id;
            *self.election.term.lock().await = term_id;
            self.election.voted_for.lock().await.take();
            self.election.votes_count.lock().await.clear();
        }

        {
            let mut votes_count = self.election.votes_count.lock().await;
            if let Entry::Vacant(entry) = votes_count.entry(candidate_id) {
                let mut votes = HashSet::new();
                votes.insert(node_id);
                entry.insert(votes);
                info!("Initial vote for candidate ID: {candidate_id} from node ID: {node_id}, term: {term_id}.");
            } else {
                votes_count.get_mut(&candidate_id).unwrap().insert(node_id);
                info!("Additional vote for candidate ID: {candidate_id} from node ID: {node_id}, term: {term_id}.");
            }
        }

        let mut voted_for = self.election.voted_for.lock().await;
        if self.self_id == node_id && voted_for.is_none() {
            voted_for.replace(candidate_id);
            info!("You have voted for: {candidate_id}, term: {term_id}.")
        }

        self.count_votes(term_id).await;

        true
    }

    async fn count_votes(&self, term_id: TermId) -> ElectionState {
        info!("Counting votes for term: {term_id}...");
        let votes_count = self.election.votes_count.lock().await;
        let leader_votes = votes_count.iter().max_by_key(|(_, votes)| votes.len());
        if leader_votes.is_none() {
            return ElectionState::NoLeaderElected;
        }

        let (leader, votes) = leader_votes.unwrap();
        info!(
            "Current votes: {} for term: {term_id}, possible leader: {leader}",
            votes.len()
        );
        if votes.len() > self.required_votes_count {
            self.set_election_completed_state(true).await;
            let leader = *leader;
            self.current_leader_id.lock().await.replace(leader);
            self.election.leader_id.lock().await.replace(leader);
            info!("Election for term: {term_id} has completed, leader ID: {leader}.");
            return ElectionState::LeaderElected(leader);
        }

        ElectionState::NoLeaderElected
    }

    pub async fn is_election_completed(&self) -> bool {
        *self.election.is_completed.lock().await
    }

    pub async fn set_election_completed_state(&self, is_completed: bool) {
        *self.election.is_completed.lock().await = is_completed;
    }
}
