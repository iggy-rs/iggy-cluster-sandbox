use bytes::Bytes;
use sdk::commands::append_messages::{AppendMessages, AppendableMessage};
use sdk::commands::command::Command;
use sdk::commands::create_stream::CreateStream;
use sdk::commands::get_metadata::GetMetadata;
use sdk::commands::poll_messages::PollMessages;

pub(crate) fn parse(input: &str) -> Option<Command> {
    let parts = input.split('.').collect::<Vec<&str>>();
    let command = parts[0];
    match command {
        "metadata" => Some(GetMetadata::new_command()),
        "stream" => Some(parse_create_stream(parts[1])),
        "append" => Some(parse_append_messages(parts[1])),
        "poll" => Some(parse_poll_messages(parts[1])),
        _ => None,
    }
}

fn parse_create_stream(input: &str) -> Command {
    let id = input.parse::<u64>().unwrap();
    CreateStream::new_command(id)
}

fn parse_append_messages(input: &str) -> Command {
    let parts = input.split('|').collect::<Vec<&str>>();
    let stream_id = parts[0].parse::<u64>().unwrap();
    let messages = parts[1]
        .split(',')
        .map(|x| AppendableMessage {
            id: 0,
            payload: Bytes::from(x.as_bytes().to_vec()),
        })
        .collect::<Vec<AppendableMessage>>();

    AppendMessages::new_command(stream_id, messages)
}

fn parse_poll_messages(input: &str) -> Command {
    let parts = input.split('|').collect::<Vec<&str>>();
    let stream_id = parts[0].parse::<u64>().unwrap();
    let offset = parts[1].parse::<u64>().unwrap();
    let count = parts[2].parse::<u64>().unwrap();
    PollMessages::new_command(stream_id, offset, count)
}
