use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use crate::models::log_entry::LogEntry;
use bytes::{BufMut, Bytes};

#[derive(Debug, Default)]
pub struct AppendedState {
    pub term: u64,
    pub last_applied: u64,
    pub entries: Vec<LogEntry>,
}

impl BytesSerializable for AppendedState {
    fn as_bytes(&self) -> Vec<u8> {
        let entries_bytes = self
            .entries
            .iter()
            .map(|entry| entry.as_bytes())
            .collect::<Vec<Vec<u8>>>()
            .concat();
        let mut bytes = Vec::with_capacity(24 + entries_bytes.len());
        bytes.put_u64_le(self.term);
        bytes.put_u64_le(self.last_applied);
        bytes.put_slice(&entries_bytes);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        if bytes.len() < 16 {
            return Err(SystemError::InvalidCommand);
        }

        let term = u64::from_le_bytes(bytes[0..8].try_into()?);
        let last_applied = u64::from_le_bytes(bytes[8..16].try_into()?);
        let mut entries = Vec::new();
        let mut offset = 16;
        while offset < bytes.len() {
            let index = u64::from_le_bytes(bytes[offset..offset + 8].try_into()?);
            let size = u32::from_le_bytes(bytes[offset + 8..offset + 12].try_into()?);
            let data = Bytes::copy_from_slice(&bytes[offset + 12..offset + 12 + size as usize]);
            let entry = LogEntry { index, size, data };
            entries.push(entry);
            offset += 12 + size as usize;
        }

        Ok(AppendedState {
            term,
            last_applied,
            entries,
        })
    }
}
