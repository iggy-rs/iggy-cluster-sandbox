use crate::types::{Index, Term};
use bytes::Bytes;
use sdk::models::log_entry::LogEntry;

#[derive(Debug)]
pub struct State {
    pub term: Term,
    pub commit_index: Index,
    pub last_applied: Index,
    pub entries: Vec<LogEntry>,
}

impl State {
    pub fn new(term: Term) -> State {
        State {
            term,
            commit_index: 0,
            last_applied: 0,
            entries: vec![],
        }
    }

    pub fn set_term(&mut self, term: Term) {
        self.term = term;
    }

    pub fn append(&mut self, payload: Bytes) -> LogEntry {
        if self.commit_index > 0 || !self.entries.is_empty() {
            self.commit_index += 1;
        }
        let result = LogEntry {
            index: self.commit_index,
            data: payload.clone(),
        };
        let entry = LogEntry {
            index: self.commit_index,
            data: payload,
        };
        self.entries.push(entry);
        result
    }

    pub fn update_last_applied_to_commit_index(&mut self) {
        self.last_applied = self.commit_index;
    }
}
