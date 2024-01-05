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
    pub data: Vec<u8>,
}
