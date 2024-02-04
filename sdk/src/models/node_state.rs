#[derive(Debug, Default)]
pub struct NodeState {
    pub id: u64,
    pub address: String,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
}
