use bytes::Bytes;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Message {
    pub offset: u64,
    pub id: u64,
    pub payload: Bytes,
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Message {{ offset: {}, id: {}, payload: {} }}",
            self.offset,
            self.id,
            String::from_utf8_lossy(&self.payload)
        )
    }
}
