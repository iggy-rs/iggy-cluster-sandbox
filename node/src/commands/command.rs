use crate::bytes_serializable::BytesSerializable;
use crate::commands::hello::Hello;
use crate::commands::ping::Ping;
use crate::error::SystemError;
use bytes::BufMut;
use std::fmt::{Display, Formatter};

const HELLO_CODE: u32 = 1;
const PING_CODE: u32 = 2;

#[derive(Debug, PartialEq)]
pub enum Command {
    Hello(Hello),
    Ping(Ping),
}

impl Command {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Command::Hello(command) => map_as_bytes(HELLO_CODE, command),
            Command::Ping(command) => map_as_bytes(PING_CODE, command),
        }
    }

    pub fn from_bytes(code: u32, bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        match code {
            HELLO_CODE => Ok(Command::Hello(Hello::from_bytes(bytes)?)),
            PING_CODE => Ok(Command::Ping(Ping::from_bytes(bytes)?)),
            _ => Err(SystemError::InvalidCommandCode(code)),
        }
    }
}

fn map_as_bytes<T: BytesSerializable>(code: u32, command: &T) -> Vec<u8> {
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
        }
    }
}
