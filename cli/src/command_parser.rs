use bytes::Bytes;
use sdk::commands::append_messages::{AppendMessages, Message};
use sdk::commands::command::Command;
use sdk::commands::ping::Ping;

pub(crate) fn parse(input: &str) -> Option<Command> {
    let parts = input.split('.').collect::<Vec<&str>>();
    let command = parts[0];
    match command {
        "ping" => Some(Ping::new_command()),
        "append" => Some(parse_append_messages(parts[1])),
        _ => None,
    }
}

fn parse_append_messages(input: &str) -> Command {
    let messages = input
        .split(',')
        .map(|x| Message {
            id: 0,
            payload: Bytes::from(x.as_bytes().to_vec()),
        })
        .collect::<Vec<Message>>();

    AppendMessages::new_command(messages)
}
