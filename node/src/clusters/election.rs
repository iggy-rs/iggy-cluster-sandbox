use crate::clusters::cluster::ClusterNodeState;
use futures::lock::Mutex;
use monoio::time::sleep;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::info;

type CandidateId = u64;
type TermId = AtomicU64;

#[derive(Debug)]
pub(crate) struct Election {
    pub is_completed: Mutex<bool>,
    pub term: TermId,
    pub leader_id: Mutex<Option<CandidateId>>,
    pub votes_count: Mutex<HashMap<CandidateId, HashSet<CandidateId>>>,
    pub voted_for: Mutex<Option<CandidateId>>,
}

impl Default for Election {
    fn default() -> Self {
        Self {
            is_completed: Mutex::new(true),
            term: TermId::new(0),
            leader_id: Mutex::new(None),
            votes_count: Mutex::new(HashMap::new()),
            voted_for: Mutex::new(None),
        }
    }
}

#[derive(Debug)]
pub struct ElectionManager {
    self_id: CandidateId,
    current_term: TermId,
    current_leader_id: Option<CandidateId>,
    election: Election,
    randomizer: Mutex<ThreadRng>,
}

impl ElectionManager {
    pub fn new(self_id: CandidateId) -> Self {
        Self {
            self_id,
            current_term: AtomicU64::new(0),
            current_leader_id: None,
            election: Election::default(),
            randomizer: Mutex::new(rand::thread_rng()),
        }
    }

    pub async fn start_election(&self) -> ClusterNodeState {
        if !self.is_election_completed().await {
            return ClusterNodeState::Candidate;
        }

        self.set_election_completed_state(false).await;
        let ordering = Ordering::SeqCst;
        let current_term = self.current_term.fetch_add(1, ordering);
        let new_term = current_term + 1;
        self.election.term.store(new_term, ordering);
        let random_timeout = self.randomizer.lock().await.gen_range(150..=300);
        info!("Starting election for new term {new_term}, current term: {current_term}, timeout: {random_timeout} ms...");
        // Wait for random timeout and check if there is no leader in the meantime
        sleep(Duration::from_millis(random_timeout)).await;
        self.set_election_completed_state(true).await;
        info!("Finished election for new term {new_term}, , current term: {current_term}.");
        if self.election.leader_id.lock().await.is_none() {
            // TODO: Notify other nodes to vote for you as a leader
            self.assign_vote(self.self_id, self.self_id).await;
            return ClusterNodeState::Candidate;
        }

        let votes_count = self.election.votes_count.lock().await;
        let (leader, votes) = votes_count
            .iter()
            .max_by_key(|(_, votes)| votes.len())
            .unwrap();

        if votes.len() > (votes_count.len() / 2) {
            let leader = *leader;
            self.election.leader_id.lock().await.replace(leader);
            return if self.self_id == leader {
                ClusterNodeState::Leader
            } else {
                ClusterNodeState::Follower
            };
        }

        // TODO: No leader elected, start new election
        ClusterNodeState::Candidate
    }

    pub async fn assign_vote(&self, vote_by: CandidateId, vote_for: CandidateId) {
        if self.current_leader_id.is_some() {
            // Election is over
            return;
        }

        let mut votes_count = self.election.votes_count.lock().await;
        if let Entry::Vacant(entry) = votes_count.entry(vote_by) {
            let mut votes = HashSet::new();
            votes.insert(vote_for);
            entry.insert(votes);
        } else {
            votes_count.get_mut(&vote_by).unwrap().insert(vote_for);
        }

        let mut voted_for = self.election.voted_for.lock().await;
        if self.self_id == vote_by && voted_for.is_none() {
            voted_for.replace(vote_for);
        }
    }

    async fn is_election_completed(&self) -> bool {
        *self.election.is_completed.lock().await
    }

    async fn set_election_completed_state(&self, is_completed: bool) {
        *self.election.is_completed.lock().await = is_completed;
    }
}
