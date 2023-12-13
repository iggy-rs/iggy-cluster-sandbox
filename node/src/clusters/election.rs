use futures::lock::Mutex;
use monoio::time::sleep;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::info;

type CandidateId = u64;
type TermId = u64;

#[derive(Debug)]
pub(crate) struct Election {
    pub is_completed: Mutex<bool>,
    pub term: Mutex<TermId>,
    pub leader_id: Mutex<Option<CandidateId>>,
    pub votes_count: Mutex<HashMap<CandidateId, HashSet<CandidateId>>>,
    pub voted_for: Mutex<Option<CandidateId>>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ElectionState {
    InProgress,
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
    current_term: TermId,
    current_leader_id: Option<CandidateId>,
    election: Election,
    randomizer: Mutex<ThreadRng>,
}

impl ElectionManager {
    pub fn new(self_id: CandidateId) -> Self {
        Self {
            self_id,
            current_term: 0,
            current_leader_id: None,
            election: Election::default(),
            randomizer: Mutex::new(rand::thread_rng()),
        }
    }

    pub fn next_term(&self) -> TermId {
        self.current_term + 1
    }

    pub async fn start_election(&self, term: TermId) -> ElectionState {
        if !self.is_election_completed().await {
            return ElectionState::InProgress;
        }

        *self.election.term.lock().await = term;
        let current_term = self.current_term;
        self.set_election_completed_state(false).await;
        let random_timeout = self.randomizer.lock().await.gen_range(150..=300);
        info!("Starting election for new term {term}, current term: {current_term}, timeout: {random_timeout} ms...");
        // Wait for random timeout and check if there is no leader in the meantime
        sleep(Duration::from_millis(random_timeout)).await;
        self.set_election_completed_state(true).await;
        info!("Finished election for new term {term}, current term: {current_term}.");
        if self.election.leader_id.lock().await.is_none() {
            self.assign_vote(self.self_id, self.self_id).await;
            return ElectionState::NoLeaderElected;
        }

        let votes_count = self.election.votes_count.lock().await;
        let (leader, votes) = votes_count
            .iter()
            .max_by_key(|(_, votes)| votes.len())
            .unwrap();

        if votes.len() > (votes_count.len() / 2) {
            let leader = *leader;
            self.election.leader_id.lock().await.replace(leader);
            return ElectionState::LeaderElected(leader);
        }

        ElectionState::NoLeaderElected
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
