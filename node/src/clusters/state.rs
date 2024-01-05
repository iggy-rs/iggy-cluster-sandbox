use crate::types::{Index, Term};

#[derive(Debug)]
pub struct State {
    pub term: Term,
    pub commit_index: Index,
    pub last_applied: Index,
}

impl State {
    pub fn new(term: Term) -> State {
        State {
            term,
            commit_index: 0,
            last_applied: 0,
        }
    }

    pub fn set_term(&mut self, term: Term) {
        self.term = term;
    }

    pub fn increase_commit_index(&mut self) {
        self.commit_index += 1;
    }

    pub fn update_last_applied_to_commit_index(&mut self) {
        self.last_applied = self.commit_index;
    }
}
