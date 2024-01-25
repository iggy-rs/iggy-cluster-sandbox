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
    pub high_water_mark: Index,
    pub entries: Vec<LogEntry>,
    current_position: u64,
    directory_path: String,
    log_path: String,
    high_watermark_path: String,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut entries = String::new();
        for entry in &self.entries {
            entries.push_str(&format!("{}\n", entry));
        }
        write!(
            f,
            "term: {}, commit_index: {}, last_applied: {}, high_water_mark: {}, entries: {}",
            self.term, self.commit_index, self.last_applied, self.high_water_mark, entries
        )
    }
}

impl State {
    pub fn new(term: Term, path: &str) -> State {
        State {
            term,
            commit_index: 0,
            last_applied: 0,
            high_water_mark: 0,
            current_position: 0,
            entries: vec![],
            directory_path: path.to_string(),
            log_path: format!("{}/state.log", path),
            high_watermark_path: format!("{}/high_watermark", path),
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

        if !Path::new(&self.high_watermark_path).exists() {
            file::write(&self.high_watermark_path)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "Failed to create high watermark file: {}",
                        self.high_watermark_path
                    )
                });
            let file = file::append(&self.high_watermark_path).await.unwrap();
            if file.write_all_at(vec![0u8; 8], 0).await.0.is_err() {
                error!("Failed to init high watermark");
                return;
            }
            info!(
                "Created empty high watermark file: {}",
                self.high_watermark_path
            );
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
            self.commit_index = index;
            self.entries.push(entry);
        }

        self.current_position = position;
        self.last_applied = self.commit_index;

        info!(
            "Initialized state for {} entries. {}",
            self.entries.len(),
            self
        );

        let file = file::open(&self.high_watermark_path).await.unwrap();
        let buffer = vec![0u8; 8];
        let (result, buffer) = file.read_exact_at(buffer, 0).await;
        if result.is_err() {
            error!("Failed to read high watermark");
            return;
        }

        let high_water_mark = u64::from_le_bytes(buffer.try_into().unwrap());
        self.high_water_mark = high_water_mark;
        info!("Initialized high watermark: {}", self.high_water_mark);
    }

    pub fn set_term(&mut self, term: Term) {
        self.term = term;
    }

    pub async fn append(&mut self, payload: Bytes) -> Result<LogEntry, SystemError> {
        if self.commit_index > 0 || !self.entries.is_empty() {
            self.commit_index += 1;
        }
        let entry = LogEntry {
            index: self.commit_index,
            data: payload.clone(),
        };
        self.sync(LogEntry {
            index: self.commit_index,
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
        self.entries.push(entry);
        self.current_position += size as u64;
        Ok(())
    }

    pub fn update_last_applied_to_commit_index(&mut self) {
        self.last_applied = self.commit_index;
    }

    pub async fn set_high_water_mark(&mut self, high_water_mark: Index) {
        self.high_water_mark = high_water_mark;
        let file = file::write(&self.high_watermark_path).await.unwrap();
        if file
            .write_all_at(high_water_mark.to_le_bytes().to_vec(), 0)
            .await
            .0
            .is_err()
        {
            error!("Failed to write high watermark");
            return;
        }
        info!("Saved high watermark: {}", self.high_water_mark);
    }
}
