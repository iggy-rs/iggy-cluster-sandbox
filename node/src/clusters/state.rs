use crate::streaming::file;
use crate::types::{Index, Term};
use bytes::{BufMut, Bytes};
use log::error;
use sdk::models::log_entry::LogEntry;
use std::fmt::Display;
use std::fs::create_dir_all;
use std::path::Path;
use tracing::info;

#[derive(Debug)]
pub struct State {
    pub term: Term,
    pub commit_index: Index,
    pub last_applied: Index,
    pub entries: Vec<LogEntry>,
    current_position: u64,
    directory_path: String,
    log_path: String,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut entries = String::new();
        for entry in &self.entries {
            entries.push_str(&format!("{}\n", entry));
        }
        write!(
            f,
            "term: {}, commit_index: {}, last_applied: {}, entries: {}",
            self.term, self.commit_index, self.last_applied, entries
        )
    }
}

impl State {
    pub fn new(term: Term, path: &str) -> State {
        State {
            term,
            commit_index: 0,
            last_applied: 0,
            current_position: 0,
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

        info!("Initializing state...");
        let file = file::open(&self.log_path).await.unwrap();
        let mut position = 0u64;
        loop {
            let buffer = vec![0u8; 8];
            let (result, buffer) = file.read_exact_at(buffer, position).await;
            if result.is_err() {
                error!("Failed to read index");
                break;
            }

            let index = u64::from_le_bytes(buffer.try_into().unwrap());
            position += 8;

            let buffer = vec![0u8; 8];
            let (result, buffer) = file.read_exact_at(buffer, position).await;
            if result.is_err() {
                error!("Failed to read term");
                break;
            }

            let _term = u64::from_le_bytes(buffer.try_into().unwrap());
            position += 8;

            let buffer = vec![0u8; 4];
            let (result, buffer) = file.read_exact_at(buffer, position).await;
            if result.is_err() {
                error!("Failed to read payload length");
                break;
            }

            let data_length = u32::from_le_bytes(buffer.try_into().unwrap());
            position += 4;

            let buffer = vec![0; data_length as usize];
            let (data_result, data) = file.read_exact_at(buffer, position).await;
            if data_result.is_err() {
                error!("Failed to read payload");
                break;
            }

            let data = Bytes::from(data);
            position += data_length as u64;
            let entry = LogEntry { index, data };
            self.entries.push(entry);
        }

        self.current_position = position;
        info!(
            "Initialized state for {} entries. {}",
            self.entries.len(),
            self
        );
    }

    pub fn set_term(&mut self, term: Term) {
        self.term = term;
    }

    pub async fn append(&mut self, payload: Bytes) -> LogEntry {
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
        let file = file::append(&self.log_path).await.unwrap();
        let size = 20 + result.data.len();
        let mut bytes = Vec::with_capacity(size);
        bytes.put_u64_le(result.index);
        bytes.put_u64_le(self.term);
        bytes.put_u32_le(result.data.len() as u32);
        bytes.put_slice(&result.data);
        let (write_result, _) = file.write_all_at(bytes, self.current_position).await;
        if write_result.is_err() {
            panic!("Failed to write to state file: {:?}", write_result.err());
        }

        info!(
            "Appended entry at position: {}, size: {size}",
            self.current_position
        );
        self.current_position += size as u64;
        result
    }

    pub fn update_last_applied_to_commit_index(&mut self) {
        self.last_applied = self.commit_index;
    }
}
