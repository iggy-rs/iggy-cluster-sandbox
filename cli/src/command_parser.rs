use bytes::Bytes;
use sdk::commands::append_messages::{AppendMessages, AppendableMessage};
use sdk::commands::command::Command;
use sdk::commands::create_stream::CreateStream;
use sdk::commands::get_metadata::GetMetadata;
use sdk::commands::get_streams::GetStreams;
use sdk::commands::ping::Ping;
use sdk::commands::poll_messages::PollMessages;

pub(crate) fn parse(input: &str) -> Option<Command> {
    let parts = input.split('.').collect::<Vec<&str>>();
    let command = parts[0];
    match command {
        "metadata" => Some(GetMetadata::new_command()),
        "ping" => Some(Ping::new_command()),
        "streams" => Some(GetStreams::new_command()),
        "stream" => parse_create_stream(parts.get(1).unwrap_or(&"")),
        "append" => parse_append_messages(parts.get(1).unwrap_or(&"")),
        "poll" => parse_poll_messages(parts.get(1).unwrap_or(&"")),
        _ => None,
    }
}

fn parse_create_stream(input: &str) -> Option<Command> {
    let id = input.parse::<u64>().unwrap();
    Some(CreateStream::new_command(id))
}

fn parse_append_messages(input: &str) -> Option<Command> {
    let parts = input.split('|').collect::<Vec<&str>>();
    if parts.len() != 2 {
        return None;
    }

    let stream_id = parts[0].parse::<u64>().unwrap();
    let messages = parts[1]
        .split(',')
        .map(|x| AppendableMessage {
            id: 0,
            payload: Bytes::from(x.as_bytes().to_vec()),
        })
        .collect::<Vec<AppendableMessage>>();

    Some(AppendMessages::new_command(stream_id, messages))
}

fn parse_poll_messages(input: &str) -> Option<Command> {
    let parts = input.split('|').collect::<Vec<&str>>();
    if parts.len() != 3 {
        return None;
    }

    let stream_id = parts[0].parse::<u64>().unwrap();
    let offset = parts[1].parse::<u64>().unwrap();
    let count = parts[2].parse::<u64>().unwrap();
    Some(PollMessages::new_command(stream_id, offset, count))
}
