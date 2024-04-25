use crate::streaming::file;
use crate::types::{Index, Term};
use bytes::{BufMut, Bytes};
use sdk::error::SystemError;
use sdk::models::log_entry::LogEntry;
use std::fmt::Display;
use std::fs::create_dir_all;
use std::path::Path;
use tracing::{error, info};

#[derive(Debug)]
pub struct State {
    pub term: Term,
    pub commit_index: Index,
    pub last_applied: Index,
    current_position: u64,
    directory_path: String,
    log_path: String,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "term: {}, commit_index: {}, last_applied: {}",
            self.term, self.commit_index, self.last_applied
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
            directory_path: path.to_string(),
            log_path: format!("{}/state.log", path),
        }
    }

    pub async fn truncate(&mut self, to_index: Index) -> Result<(), SystemError> {
        self.load_and_set_state(Some(to_index))
            .await
            .unwrap_or_else(|_| panic!("Failed to load state from disk: {}", self.log_path));

        let file = std::fs::File::open(&self.log_path)?;
        let file_size = file.metadata()?.len();
        if file_size > self.current_position {
            info!(
                "Truncating state file from: {file_size} bytes to {} bytes...",
                self.current_position
            );
            file.set_len(self.current_position)?;
        }

        Ok(())
    }

    pub async fn init(&mut self) {
        info!("Initializing state...");
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
        self.load_and_set_state(None)
            .await
            .unwrap_or_else(|_| panic!("Failed to load state from disk: {}", self.log_path));

        info!("Initialized state");
    }

    pub async fn load_entries(&self, to_index: Option<Index>) -> Vec<LogEntry> {
        info!("Loading entries from disk, to index: {:?}", to_index);
        let mut entries = vec![];
        self.load_state_from_disk(to_index, &mut |entry| entries.push(entry))
            .await;
        entries
    }

    async fn load_and_set_state(&mut self, to_index: Option<Index>) -> Result<(), SystemError> {
        let mut entries = vec![];
        let (position, term, index) = self
            .load_state_from_disk(to_index, &mut |entry| entries.push(entry))
            .await;
        self.commit_index = index;
        self.term = term;
        self.current_position = position;
        self.last_applied = self.commit_index;
        Ok(())
    }

    async fn load_state_from_disk<F>(
        &self,
        to_index: Option<u64>,
        on_entry: &mut F,
    ) -> (u64, Term, Index)
    where
        F: FnMut(LogEntry),
    {
        let file = file::open(&self.log_path).await.unwrap();
        let mut position = 0u64;
        let mut term = 0;
        let mut index = 0;
        loop {
            let buffer = vec![0u8; 8];
            let (result, buffer) = file.read_exact_at(buffer, position).await;
            if result.is_err() {
                error!("Failed to read index");
                break;
            }

            index = u64::from_le_bytes(buffer.try_into().unwrap());
            position += 8;

            let buffer = vec![0u8; 8];
            let (result, buffer) = file.read_exact_at(buffer, position).await;
            if result.is_err() {
                error!("Failed to read term");
                break;
            }

            term = u64::from_le_bytes(buffer.try_into().unwrap());
            position += 8;

            let buffer = vec![0u8; 4];
            let (result, buffer) = file.read_exact_at(buffer, position).await;
            if result.is_err() {
                error!("Failed to read payload length");
                break;
            }

            let size = u32::from_le_bytes(buffer.try_into().unwrap());
            position += 4;

            let buffer = vec![0; size as usize];
            let (data_result, data) = file.read_exact_at(buffer, position).await;
            if data_result.is_err() {
                error!("Failed to read payload");
                break;
            }

            let data = Bytes::from(data);
            position += size as u64;
            let entry = LogEntry { index, size, data };
            on_entry(entry);

            if let Some(to_index) = to_index {
                if index >= to_index {
                    break;
                }
            }
        }

        (position, term, index)
    }

    pub fn set_term(&mut self, term: Term) {
        self.term = term;
    }

    pub async fn append(&mut self, payload: Bytes) -> Result<LogEntry, SystemError> {
        if self.commit_index > 0 {
            self.commit_index += 1;
        }
        let entry = LogEntry {
            index: self.commit_index,
            size: payload.len() as u32,
            data: payload.clone(),
        };
        self.sync(LogEntry {
            index: self.commit_index,
            size: payload.len() as u32,
            data: payload,
        })
        .await?;
        Ok(entry)
    }

    pub async fn sync(&mut self, entry: LogEntry) -> Result<(), SystemError> {
        let file = file::append(&self.log_path).await.unwrap();
        let size = 20 + entry.data.len();
        let mut bytes = Vec::with_capacity(size);
        bytes.put_u64_le(entry.index);
        bytes.put_u64_le(self.term);
        bytes.put_u32_le(entry.data.len() as u32);
        bytes.put_slice(&entry.data);
        if file
            .write_all_at(bytes, self.current_position)
            .await
            .0
            .is_err()
        {
            return Err(SystemError::CannotAppendToState);
        }
        info!(
            "Appended entry at position: {}, size: {size}",
            self.current_position
        );
        self.current_position += size as u64;
        Ok(())
    }

    pub fn update_last_applied_to_commit_index(&mut self) {
        self.last_applied = self.commit_index;
    }
}
