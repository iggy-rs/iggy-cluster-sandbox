use crate::command::Command;
use crate::error::SystemError;
use crate::server::tcp_sender::TcpSender;
use std::io::ErrorKind;
use tracing::{debug, error, info};

const INITIAL_BYTES_LENGTH: usize = 8;

pub(crate) async fn handle_connection(sender: &mut TcpSender) -> Result<(), SystemError> {
    let mut initial_buffer = Vec::with_capacity(INITIAL_BYTES_LENGTH);
    let mut read_length = 0;
    initial_buffer.fill_with(|| 0u8);
    loop {
        (read_length, initial_buffer) = sender.read(initial_buffer).await?;
        if read_length != INITIAL_BYTES_LENGTH {
            error!(
                "Unable to read the TCP request length, expected: {} bytes, received: {} bytes.",
                INITIAL_BYTES_LENGTH, read_length
            );
            continue;
        }

        let command_code = u32::from_le_bytes(initial_buffer[..4].try_into()?);
        let command = Command::from_code(command_code)?;
        let length = u32::from_le_bytes(initial_buffer[4..8].try_into()?);
        debug!("Received a TCP request, command: {command}, length: {length}");
        if length > 0 {
            let buffer = vec![0u8; length as usize];
            sender.read(buffer).await?;
        }

        match command {
            Command::Ping => {
                info!("Received a TCP ping request.");
                sender.send_empty_ok_response().await?;
            }
        }
        info!("Sent a TCP response.");
    }
}

pub(crate) fn handle_error(error: SystemError) {
    match error {
        SystemError::IoError(error) => match error.kind() {
            ErrorKind::UnexpectedEof => {
                info!("Connection has been closed.");
            }
            ErrorKind::ConnectionAborted => {
                info!("Connection has been aborted.");
            }
            ErrorKind::ConnectionRefused => {
                info!("Connection has been refused.");
            }
            ErrorKind::ConnectionReset => {
                info!("Connection has been reset.");
            }
            _ => {
                error!("Connection has failed: {error}");
            }
        },
        _ => {
            error!("Connection has failed: {error}");
        }
    }
}
