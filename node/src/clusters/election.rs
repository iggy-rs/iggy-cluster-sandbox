#[derive(Debug)]
pub struct Election {
    pub term: u64,
    pub leader: String,
    pub votes_count: u64,
    pub voted_for: String,
}

impl Election {
    pub fn new() -> Self {
        Self {
            term: 0,
            leader: "".to_string(),
            votes_count: 0,
            voted_for: "".to_string(),
        }
    }
}
