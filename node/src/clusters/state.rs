use crate::streaming::file;
use crate::types::{Index, Term};
use bytes::Bytes;
use sdk::models::log_entry::LogEntry;
use std::fs::create_dir_all;
use std::path::Path;
use tracing::info;

#[derive(Debug)]
pub struct State {
    pub term: Term,
    pub commit_index: Index,
    pub last_applied: Index,
    pub entries: Vec<LogEntry>,
    directory_path: String,
    log_path: String,
}

impl State {
    pub fn new(term: Term, path: &str) -> State {
        State {
            term,
            commit_index: 0,
            last_applied: 0,
            entries: vec![],
            directory_path: path.to_string(),
            log_path: format!("{}/state.log", path),
        }
    }

    pub async fn init(&mut self) {
        if !Path::new(&self.directory_path).exists() {
            create_dir_all(&self.directory_path).unwrap_or_else(|_| {
                panic!("Failed to create state directory: {}", self.directory_path)
            });
            info!("Created state directory: {}", self.directory_path);
        }
        if !Path::new(&self.log_path).exists() {
            file::write(&self.log_path)
                .await
                .unwrap_or_else(|_| panic!("Failed to create state file: {}", self.log_path));
            info!("Created empty state file: {}", self.log_path);
        }
        info!("Initialized state.");
        // TODO: Load state from disk.
    }

    pub fn set_term(&mut self, term: Term) {
        self.term = term;
    }

    // TODO: Persist state to disk.
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
