use crate::bytes_serializable::BytesSerializable;
use crate::commands::append_messages::AppendMessages;
use crate::commands::create_stream::CreateStream;
use crate::commands::get_metadata::GetMetadata;
use crate::commands::hello::Hello;
use crate::commands::ping::Ping;
use crate::commands::poll_messages::PollMessages;
use crate::commands::request_vote::RequestVote;
use crate::commands::send_vote::SendVote;
use crate::commands::sync_messages::SyncMessages;
use crate::error::SystemError;
use bytes::BufMut;
use std::fmt::{Display, Formatter};

const HELLO_CODE: u32 = 1;
const PING_CODE: u32 = 2;
const REQUEST_VOTE_CODE: u32 = 3;
const SEND_VOTE_CODE: u32 = 4;
const GET_METADATA_CODE: u32 = 5;
const SYNC_MESSAGES_CODE: u32 = 10;
const CREATE_STREAM_CODE: u32 = 20;
const APPEND_MESSAGES_CODE: u32 = 30;
const POLL_MESSAGES_CODE: u32 = 40;

#[derive(Debug)]
pub enum Command {
    Hello(Hello),
    Ping(Ping),
    RequestVote(RequestVote),
    SendVote(SendVote),
    GetMetadata(GetMetadata),
    CreateStream(CreateStream),
    AppendMessages(AppendMessages),
    PollMessages(PollMessages),
    SyncMessages(SyncMessages),
}

impl Command {
    pub fn get_name(&self) -> &str {
        match self {
            Command::Hello(_) => "hello",
            Command::Ping(_) => "ping",
            Command::RequestVote(_) => "request_vote",
            Command::SendVote(_) => "send_vote",
            Command::GetMetadata(_) => "get_metadata",
            Command::CreateStream(_) => "create_stream",
            Command::AppendMessages(_) => "append_messages",
            Command::PollMessages(_) => "poll_messages",
            Command::SyncMessages(_) => "sync_messages",
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Command::Hello(command) => to_bytes(HELLO_CODE, command),
            Command::Ping(command) => to_bytes(PING_CODE, command),
            Command::RequestVote(command) => to_bytes(REQUEST_VOTE_CODE, command),
            Command::SendVote(command) => to_bytes(SEND_VOTE_CODE, command),
            Command::GetMetadata(command) => to_bytes(GET_METADATA_CODE, command),
            Command::CreateStream(command) => to_bytes(CREATE_STREAM_CODE, command),
            Command::AppendMessages(command) => to_bytes(APPEND_MESSAGES_CODE, command),
            Command::PollMessages(command) => to_bytes(POLL_MESSAGES_CODE, command),
            Command::SyncMessages(command) => to_bytes(SYNC_MESSAGES_CODE, command),
        }
    }

    pub fn from_bytes(code: u32, bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        match code {
            HELLO_CODE => Ok(Command::Hello(Hello::from_bytes(bytes)?)),
            PING_CODE => Ok(Command::Ping(Ping::from_bytes(bytes)?)),
            REQUEST_VOTE_CODE => Ok(Command::RequestVote(RequestVote::from_bytes(bytes)?)),
            SEND_VOTE_CODE => Ok(Command::SendVote(SendVote::from_bytes(bytes)?)),
            GET_METADATA_CODE => Ok(Command::GetMetadata(GetMetadata::from_bytes(bytes)?)),
            CREATE_STREAM_CODE => Ok(Command::CreateStream(CreateStream::from_bytes(bytes)?)),
            APPEND_MESSAGES_CODE => Ok(Command::AppendMessages(AppendMessages::from_bytes(bytes)?)),
            POLL_MESSAGES_CODE => Ok(Command::PollMessages(PollMessages::from_bytes(bytes)?)),
            SYNC_MESSAGES_CODE => Ok(Command::SyncMessages(SyncMessages::from_bytes(bytes)?)),
            _ => Err(SystemError::InvalidCommandCode(code)),
        }
    }
}

fn to_bytes<T: BytesSerializable>(code: u32, command: &T) -> Vec<u8> {
    let bytes = command.as_bytes();
    let mut command = Vec::with_capacity(8 + bytes.len());
    command.put_u32_le(code);
    command.put_u32_le(bytes.len() as u32);
    command.extend(&bytes);
    command
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Hello(hello) => write!(f, "Hello from: {}", hello.name),
            Command::Ping(_) => write!(f, "Ping"),
            Command::RequestVote(request_vote) => {
                write!(
                    f,
                    "Request vote: {}, {}",
                    request_vote.term, request_vote.candidate_id
                )
            }
            Command::SendVote(send_vote) => {
                write!(
                    f,
                    "Send vote: {}, {}",
                    send_vote.term, send_vote.candidate_id
                )
            }
            Command::GetMetadata(_) => write!(f, "Get metadata"),
            Command::CreateStream(create_stream) => {
                write!(f, "Create stream: {}", create_stream.id)
            }
            Command::AppendMessages(append_data) => {
                write!(f, "Append messages: {:?}", append_data.messages)
            }
            Command::PollMessages(poll_data) => {
                write!(
                    f,
                    "Poll messages -> offset: {}, count: {}",
                    poll_data.offset, poll_data.count
                )
            }
            Command::SyncMessages(sync_data) => {
                write!(f, "Sync messages: {:?}", sync_data.messages)
            }
        }
    }
}
