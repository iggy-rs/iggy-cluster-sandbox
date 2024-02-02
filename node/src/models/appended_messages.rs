use sdk::models::message::Message;

#[derive(Debug)]
pub struct AppendedMessages {
    pub uncommited_messages: Vec<Message>,
    pub previous_offset: u64,
}

impl AppendedMessages {
    pub fn new(uncommited_messages: Vec<Message>, previous_offset: u64) -> AppendedMessages {
        AppendedMessages {
            uncommited_messages,
            previous_offset,
        }
    }
}
