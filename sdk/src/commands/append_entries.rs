use crate::bytes_serializable::BytesSerializable;
use crate::commands::command::Command;
use crate::commands::create_stream::{CreateStream, CREATE_STREAM_CODE};
use crate::error::SystemError;
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

#[derive(Debug)]
pub struct LogEntry {
    pub index: u64,
    pub code: u32,
    pub data: Vec<u8>,
}

impl AppendEntries {
    pub fn from_create_stream(
        term: u64,
        leader_id: u64,
        leader_commit: u64,
        create_stream: CreateStream,
    ) -> Command {
        Command::AppendEntries(AppendEntries {
            term,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit,
            entries: vec![LogEntry {
                index: 0,
                code: CREATE_STREAM_CODE,
                data: create_stream.as_bytes(),
            }],
        })
    }
}

impl BytesSerializable for LogEntry {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12 + self.data.len());
        bytes.put_u64_le(self.index);
        bytes.put_u32_le(self.code);
        bytes.extend(&self.data);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let index = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let code = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let data = bytes[12..].to_vec();
        Ok(LogEntry { index, code, data })
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
            position += 12 + entry.data.len();
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
