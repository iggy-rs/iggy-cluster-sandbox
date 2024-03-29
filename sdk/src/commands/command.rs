use crate::bytes_serializable::BytesSerializable;
use crate::commands::append_entries::AppendEntries;
use crate::commands::append_messages::AppendMessages;
use crate::commands::create_stream::{CreateStream, CREATE_STREAM_CODE};
use crate::commands::delete_stream::{DeleteStream, DELETE_STREAM_CODE};
use crate::commands::get_metadata::GetMetadata;
use crate::commands::get_node_state::GetNodeState;
use crate::commands::get_streams::GetStreams;
use crate::commands::heartbeat::Heartbeat;
use crate::commands::hello::Hello;
use crate::commands::load_state::LoadState;
use crate::commands::ping::Ping;
use crate::commands::poll_messages::PollMessages;
use crate::commands::request_vote::RequestVote;
use crate::commands::sync_messages::SyncMessages;
use crate::commands::update_leader::UpdateLeader;
use crate::error::SystemError;
use bytes::BufMut;
use std::fmt::{Display, Formatter};

const HELLO_CODE: u32 = 1;
const HEARTBEAT_CODE: u32 = 2;
const PING_CODE: u32 = 3;
const GET_NODE_STATE_CODE: u32 = 4;
const LOAD_STATE_CODE: u32 = 5;
const GET_METADATA_CODE: u32 = 6;
const REQUEST_VOTE_CODE: u32 = 10;
const UPDATE_LEADER_CODE: u32 = 11;
const SYNC_MESSAGES_CODE: u32 = 20;
const GET_STREAMS_CODE: u32 = 30;
const APPEND_MESSAGES_CODE: u32 = 40;
const POLL_MESSAGES_CODE: u32 = 50;
const APPEND_ENTRIES_CODE: u32 = 60;

#[derive(Debug)]
pub enum Command {
    Hello(Hello),
    Heartbeat(Heartbeat),
    Ping(Ping),
    RequestVote(RequestVote),
    UpdateLeader(UpdateLeader),
    GetNodeState(GetNodeState),
    LoadState(LoadState),
    GetMetadata(GetMetadata),
    GetStreams(GetStreams),
    CreateStream(CreateStream),
    DeleteStream(DeleteStream),
    AppendMessages(AppendMessages),
    PollMessages(PollMessages),
    SyncMessages(SyncMessages),
    AppendEntries(AppendEntries),
}

impl Command {
    pub fn get_name(&self) -> &str {
        match self {
            Command::Hello(_) => "hello",
            Command::Heartbeat(_) => "heartbeat",
            Command::Ping(_) => "ping",
            Command::RequestVote(_) => "request_vote",
            Command::UpdateLeader(_) => "update_leader",
            Command::GetNodeState(_) => "get_state",
            Command::LoadState(_) => "load_state",
            Command::GetMetadata(_) => "get_metadata",
            Command::GetStreams(_) => "get_streams",
            Command::CreateStream(_) => "create_stream",
            Command::DeleteStream(_) => "delete_stream",
            Command::AppendMessages(_) => "append_messages",
            Command::PollMessages(_) => "poll_messages",
            Command::SyncMessages(_) => "sync_messages",
            Command::AppendEntries(_) => "append_entries",
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Command::Hello(command) => to_bytes(HELLO_CODE, command),
            Command::Heartbeat(command) => to_bytes(HEARTBEAT_CODE, command),
            Command::Ping(command) => to_bytes(PING_CODE, command),
            Command::RequestVote(command) => to_bytes(REQUEST_VOTE_CODE, command),
            Command::UpdateLeader(command) => to_bytes(UPDATE_LEADER_CODE, command),
            Command::GetNodeState(command) => to_bytes(GET_NODE_STATE_CODE, command),
            Command::LoadState(command) => to_bytes(LOAD_STATE_CODE, command),
            Command::GetMetadata(command) => to_bytes(GET_METADATA_CODE, command),
            Command::GetStreams(command) => to_bytes(GET_STREAMS_CODE, command),
            Command::CreateStream(command) => to_bytes(CREATE_STREAM_CODE, command),
            Command::DeleteStream(command) => to_bytes(DELETE_STREAM_CODE, command),
            Command::AppendMessages(command) => to_bytes(APPEND_MESSAGES_CODE, command),
            Command::PollMessages(command) => to_bytes(POLL_MESSAGES_CODE, command),
            Command::SyncMessages(command) => to_bytes(SYNC_MESSAGES_CODE, command),
            Command::AppendEntries(command) => to_bytes(APPEND_ENTRIES_CODE, command),
        }
    }

    pub fn from_bytes(code: u32, bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        match code {
            HELLO_CODE => Ok(Command::Hello(Hello::from_bytes(bytes)?)),
            HEARTBEAT_CODE => Ok(Command::Heartbeat(Heartbeat::from_bytes(bytes)?)),
            PING_CODE => Ok(Command::Ping(Ping::from_bytes(bytes)?)),
            REQUEST_VOTE_CODE => Ok(Command::RequestVote(RequestVote::from_bytes(bytes)?)),
            UPDATE_LEADER_CODE => Ok(Command::UpdateLeader(UpdateLeader::from_bytes(bytes)?)),
            GET_NODE_STATE_CODE => Ok(Command::GetNodeState(GetNodeState::from_bytes(bytes)?)),
            LOAD_STATE_CODE => Ok(Command::LoadState(LoadState::from_bytes(bytes)?)),
            GET_METADATA_CODE => Ok(Command::GetMetadata(GetMetadata::from_bytes(bytes)?)),
            GET_STREAMS_CODE => Ok(Command::GetStreams(GetStreams::from_bytes(bytes)?)),
            CREATE_STREAM_CODE => Ok(Command::CreateStream(CreateStream::from_bytes(bytes)?)),
            DELETE_STREAM_CODE => Ok(Command::DeleteStream(DeleteStream::from_bytes(bytes)?)),
            APPEND_MESSAGES_CODE => Ok(Command::AppendMessages(AppendMessages::from_bytes(bytes)?)),
            POLL_MESSAGES_CODE => Ok(Command::PollMessages(PollMessages::from_bytes(bytes)?)),
            SYNC_MESSAGES_CODE => Ok(Command::SyncMessages(SyncMessages::from_bytes(bytes)?)),
            APPEND_ENTRIES_CODE => Ok(Command::AppendEntries(AppendEntries::from_bytes(bytes)?)),
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

pub fn map_from_bytes(bytes: &[u8]) -> Result<Command, SystemError> {
    let code = u32::from_le_bytes(bytes[..4].try_into()?);
    let length = u32::from_le_bytes(bytes[4..8].try_into()?);
    let payload = &bytes[8..8 + length as usize];
    Command::from_bytes(code, payload)
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Hello(hello) => write!(f, "Hello from: {}", hello.name),
            Command::Heartbeat(_) => write!(f, "Heartbeat"),
            Command::Ping(_) => write!(f, "Ping"),
            Command::RequestVote(request_vote) => {
                write!(f, "Request vote: {}", request_vote.term)
            }
            Command::UpdateLeader(update_leader) => {
                write!(f, "Update leader: {}", update_leader.term)
            }
            Command::GetNodeState(_) => write!(f, "Get node state"),
            Command::LoadState(load_state) => {
                write!(f, "Load state -> start index: {}", load_state.start_index)
            }
            Command::GetMetadata(_) => write!(f, "Get metadata"),
            Command::GetStreams(_) => write!(f, "Get streams"),
            Command::CreateStream(create_stream) => {
                write!(f, "Create stream: {}", create_stream.id)
            }
            Command::DeleteStream(delete_stream) => {
                write!(f, "Delete stream: {}", delete_stream.id)
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
            Command::AppendEntries(append_entries) => {
                write!(f, "Append entries: {:?}", append_entries)
            }
        }
    }
}
