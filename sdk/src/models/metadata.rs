#[derive(Debug)]
pub struct Metadata {
    pub nodes: Vec<NodeInfo>,
    pub streams: Vec<StreamInfo>,
}

#[derive(Debug)]
pub struct NodeInfo {
    pub id: u64,
    pub name: String,
    pub address: String,
}

#[derive(Debug)]
pub struct StreamInfo {
    pub id: u64,
    pub name: String,
    pub leader_id: u64,
}
