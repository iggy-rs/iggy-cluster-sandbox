use crate::commands::command::Command;
use crate::connection::tcp_handler::TcpHandler;
use crate::error::SystemError;
use std::io::ErrorKind;
use tracing::{debug, error, info};

const INITIAL_BYTES_LENGTH: usize = 8;

pub(crate) async fn tcp_listener(handler: &mut TcpHandler) -> Result<(), SystemError> {
    let mut initial_buffer = vec![0u8; INITIAL_BYTES_LENGTH];
    let mut read_length;
    loop {
        (read_length, initial_buffer) = handler.read(initial_buffer).await?;
        if read_length != INITIAL_BYTES_LENGTH {
            error!(
                "Unable to read the TCP request length, expected: {} bytes, received: {} bytes.",
                INITIAL_BYTES_LENGTH, read_length
            );
            return Err(SystemError::InvalidRequest);
        }

        let code = u32::from_le_bytes(initial_buffer[..4].try_into()?);
        let length = u32::from_le_bytes(initial_buffer[4..8].try_into()?);
        debug!("Received a TCP request, command code: {code}, payload length: {length}");
        if length == 0 {
            let command = Command::from_bytes(code, &[])?;
            if handle_command(handler, &command).await.is_err() {
                error!("Unable to handle the TCP request.");
            }
            continue;
        }

        let mut payload_buffer = vec![0u8; length as usize];
        (_, payload_buffer) = handler.read(payload_buffer).await?;
        let command = Command::from_bytes(code, &payload_buffer)?;
        if handle_command(handler, &command).await.is_err() {
            error!("Unable to handle the TCP request.");
        }
    }
}

async fn handle_command(handler: &mut TcpHandler, command: &Command) -> Result<(), SystemError> {
    info!("Handling a TCP request...");
    match command {
        Command::Hello(hello) => {
            info!("Received a hello command, name: {}.", hello.name);
            handler.send_empty_ok_response().await?;
            info!("Sent a hello response.");
        }
        Command::Ping(_) => {
            info!("Received a ping command.");
            handler.send_empty_ok_response().await?;
            info!("Sent a ping response.");
        }
    }
    info!("Handled a TCP request.");
    Ok(())
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
