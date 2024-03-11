use crate::types::{CandidateId, NodeId, Term};
use futures::lock::Mutex;
use monoio::time::sleep;
use rand::rngs::ThreadRng;
use rand::Rng;
use sdk::error::SystemError;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug)]
pub(crate) struct Election {
    pub is_completed: Mutex<bool>,
    pub term: Mutex<Term>,
    pub leader_id: Mutex<Option<NodeId>>,
    pub votes: Mutex<HashMap<CandidateId, HashSet<NodeId>>>,
    pub voted_for: Mutex<Option<CandidateId>>,
    pub last_heartbeat_at: Mutex<u64>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ElectionState {
    LeaderElected(u64),
    NoLeaderElected,
    TermChanged(u64),
}

impl Default for Election {
    fn default() -> Self {
        Self {
            is_completed: Mutex::new(true),
            term: Mutex::new(0),
            leader_id: Mutex::new(None),
            votes: Mutex::new(HashMap::new()),
            voted_for: Mutex::new(None),
            last_heartbeat_at: Mutex::new(0),
        }
    }
}

#[derive(Debug)]
pub struct ElectionManager {
    self_id: CandidateId,
    current_term: Mutex<Term>,
    current_leader_id: Mutex<Option<CandidateId>>,
    election: Election,
    randomizer: Mutex<ThreadRng>,
    nodes_count: u64,
    timeout_range: ElectionTimeout,
}

#[derive(Debug)]
pub struct ElectionTimeout {
    from: u64,
    to: u64,
}

impl ElectionTimeout {
    pub fn new(from: u64, to: u64) -> Self {
        if from > to {
            panic!("Invalid timeout range: {} ms > {} ms.", from, to);
        }

        Self { from, to }
    }
}

impl Display for ElectionTimeout {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} - {} ms.", self.from, self.to)
    }
}

impl ElectionManager {
    pub fn new(self_id: CandidateId, nodes_count: u64, timeout_range: ElectionTimeout) -> Self {
        info!("Election timeout range is: {timeout_range}");
        Self {
            self_id,
            current_term: Mutex::new(0),
            current_leader_id: Mutex::new(None),
            election: Election::default(),
            randomizer: Mutex::new(rand::thread_rng()),
            nodes_count,
            timeout_range,
        }
    }

    pub async fn get_last_heartbeat(&self) -> u64 {
        *self.election.last_heartbeat_at.lock().await
    }

    pub async fn set_last_heartbeat(&self, timestamp: u64) {
        *self.election.last_heartbeat_at.lock().await = timestamp;
    }

    pub async fn get_leader_id(&self) -> Option<NodeId> {
        *self.current_leader_id.lock().await
    }

    pub async fn set_term(&self, term: Term) {
        let current_term = *self.current_term.lock().await;
        if term > current_term {
            info!("Setting term: {term}...");
            *self.current_term.lock().await = term;
        }
    }

    pub fn get_quorum_count(&self) -> u64 {
        self.nodes_count / 2 + 1
    }

    pub async fn set_leader(&self, term: Term, leader_id: CandidateId) -> Result<(), SystemError> {
        let current_term = *self.current_term.lock().await;
        if current_term == term {
            if let Some(current_leader_id) = self.get_leader_id().await {
                if current_leader_id == leader_id {
                    return Ok(());
                }
            }
        }

        if term < current_term {
            warn!(
                "Received leader ID: {leader_id} in term: {term}, but current term is: {current_term}.",
            );
            return Err(SystemError::LeaderRejected);
        }

        if let Some(current_leader_id) = self.get_leader_id().await {
            warn!("Leader ID: {current_leader_id} already elected in term: {term}, ignoring leader ID: {leader_id}.",);
            return Ok(());
        }

        self.set_election_completed_state(true).await;
        *self.current_term.lock().await = term;
        self.current_leader_id.lock().await.replace(leader_id);
        info!("Leader ID: {leader_id} has been set in term: {term}.");
        Ok(())
    }

    pub async fn get_current_term(&self) -> Term {
        *self.current_term.lock().await
    }

    pub async fn next_term(&self) -> Term {
        let current_term = self.get_current_term().await;
        current_term + 1
    }

    pub async fn start_election(&self, term: Term) -> ElectionState {
        self.remove_leader().await;
        self.set_election_completed_state(false).await;
        *self.election.term.lock().await = term;
        self.election.voted_for.lock().await.take();
        self.election.votes.lock().await.clear();
        let previous_term;

        {
            previous_term = *self.current_term.lock().await;
            *self.current_term.lock().await = term;
        }

        let timeout = self
            .randomizer
            .lock()
            .await
            .gen_range(self.timeout_range.from..=self.timeout_range.to);
        info!("Starting election for new term {term}, previous term: {previous_term}, required votes: {} timeout: {timeout} ms...", self.get_quorum_count());
        // Wait for random timeout and check if there is no leader in the meantime
        sleep(Duration::from_millis(timeout)).await;
        let current_term = *self.current_term.lock().await;
        if current_term > term {
            self.set_election_completed_state(true).await;
            *self.election.term.lock().await = term;
            *self.current_term.lock().await = current_term;
            warn!("Election timeout has passed, but current term: {current_term} is greater than term: {term}, ignoring election timeout.",);
            return ElectionState::TermChanged(current_term);
        }

        info!("Election timeout has passed, checking if there is no leader in term: {term}...");
        self.count_votes().await
    }

    pub async fn vote(
        &self,
        term: Term,
        candidate_id: CandidateId,
        node_id: NodeId,
    ) -> Result<(), SystemError> {
        let current_term = *self.current_term.lock().await;
        if term < current_term {
            warn!(
                "Trying to vote in term: {term}, candidate ID: {candidate_id} from node ID: {node_id}, but current term is: {current_term}.",
            );
            return Err(SystemError::InvalidTerm(current_term));
        }

        if current_term == term {
            if self.is_election_completed().await {
                warn!("Election is over, ignoring vote request in term: {term}, candidate ID: {candidate_id} from node ID: {node_id}.");
                return Err(SystemError::ElectionsOver);
            }

            if self.get_leader_id().await.is_some() {
                warn!("Leader already elected, ignoring vote request in term: {term}, candidate ID: {candidate_id} from node ID: {node_id}.");
                return Err(SystemError::LeaderAlreadyElected);
            }
        }

        info!(
            "Voting in term: {term}, current term: {current_term} for candidate ID: {candidate_id} from node ID: {node_id}.",
        );

        if current_term < term {
            info!("Updating current term to: {term} and resetting previous votes...");
            self.current_leader_id.lock().await.take();
            *self.current_term.lock().await = term;
            *self.election.term.lock().await = term;
            self.election.voted_for.lock().await.take();
            self.election.votes.lock().await.clear();
        }

        {
            let mut votes_count = self.election.votes.lock().await;
            let already_voted = votes_count.values().any(|votes| votes.contains(&node_id));
            if already_voted {
                if self.self_id == node_id {
                    warn!(
                        "You have already voted in term: {term} for candidate ID: {candidate_id}."
                    );
                } else {
                    warn!("Node ID: {node_id} has already voted in term: {term} for candidate ID: {candidate_id}.");
                }
                return Err(SystemError::AlreadyVoted);
            }

            if let Entry::Vacant(entry) = votes_count.entry(candidate_id) {
                let mut votes = HashSet::new();
                votes.insert(node_id);
                entry.insert(votes);
                info!("Initial vote for candidate ID: {candidate_id} from node ID: {node_id} in term: {term}.");
            } else {
                votes_count.get_mut(&candidate_id).unwrap().insert(node_id);
                info!("Additional vote for candidate ID: {candidate_id} from node ID: {node_id} in term: {term}.");
            }
        }

        let mut voted_for = self.election.voted_for.lock().await;
        if self.self_id == node_id && voted_for.is_none() {
            voted_for.replace(candidate_id);
            info!("You have voted for node ID: {candidate_id} in term: {term}.")
        }

        self.count_votes().await;
        Ok(())
    }

    async fn count_votes(&self) -> ElectionState {
        let term = *self.current_term.lock().await;
        let leader_id = *self.current_leader_id.lock().await;
        if let Some(leader_id) = leader_id {
            self.set_election_completed_state(true).await;
            warn!("Leader already elected in term: {term} with ID: {leader_id}.");
            return ElectionState::LeaderElected(leader_id);
        }

        info!("Counting votes in term: {term}...");
        let votes_count = self.election.votes.lock().await;
        let leader_votes = votes_count.iter().max_by_key(|(_, votes)| votes.len());
        if leader_votes.is_none() {
            info!("No leader elected in term: {term}.");
            return ElectionState::NoLeaderElected;
        }

        let (leader, votes) = leader_votes.unwrap();
        info!(
            "Most votes: {} in term: {term}, for node ID: {leader}",
            votes.len()
        );
        if votes.len() as u64 > self.get_quorum_count() {
            self.set_election_completed_state(true).await;
            let leader = *leader;
            self.current_leader_id.lock().await.replace(leader);
            self.election.leader_id.lock().await.replace(leader);
            info!("Election in term: {term} has completed, leader ID: {leader}.");
            return ElectionState::LeaderElected(leader);
        }

        ElectionState::NoLeaderElected
    }

    pub async fn remove_leader(&self) {
        self.election.leader_id.lock().await.take();
        self.current_leader_id.lock().await.take();
    }

    pub async fn has_majority_votes(&self, term: Term, candidate_id: CandidateId) -> bool {
        if *self.current_term.lock().await != term {
            return false;
        }

        let votes_count = self.election.votes.lock().await;
        let candidate_votes = votes_count.get(&candidate_id);
        if candidate_votes.is_none() {
            return false;
        }

        let candidate_votes = candidate_votes.unwrap();
        candidate_votes.len() as u64 >= self.get_quorum_count()
    }

    pub async fn is_election_completed(&self) -> bool {
        *self.election.is_completed.lock().await
    }

    pub async fn set_election_completed_state(&self, is_completed: bool) {
        *self.election.is_completed.lock().await = is_completed;
    }
}
