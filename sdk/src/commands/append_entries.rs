use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::error::SystemError;
use crate::models::log_entry::LogEntry;
use bytes::BufMut;

#[derive(Debug)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub leader_commit: u64,
    pub entries: Vec<LogEntry>,
}

impl AppendEntries {
    pub fn new_command(
        term: u64,
        leader_id: u64,
        leader_commit: u64,
        entries: Vec<LogEntry>,
    ) -> Command {
        Command::AppendEntries(AppendEntries {
            term,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit,
            entries,
        })
    }
}

impl BytesSerializable for AppendEntries {
    fn as_bytes(&self) -> Vec<u8> {
        let entries_bytes = self.entries.iter().flat_map(|e| e.as_bytes());
        let mut bytes = vec![];
        bytes.put_u64_le(self.term);
        bytes.put_u64_le(self.leader_id);
        bytes.put_u64_le(self.prev_log_index);
        bytes.put_u64_le(self.prev_log_term);
        bytes.put_u64_le(self.leader_commit);
        bytes.extend(entries_bytes);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let term = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let leader_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let prev_log_index = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let prev_log_term = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let leader_commit = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let mut entries = Vec::new();
        let mut position = 40;
        while position < bytes.len() {
            let entry = LogEntry::from_bytes(&bytes[position..])?;
            position += 8 + entry.data.len();
            entries.push(entry);
        }
        Ok(AppendEntries {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            entries,
        })
    }
}
