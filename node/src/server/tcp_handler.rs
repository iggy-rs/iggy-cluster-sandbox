use crate::error::SystemError;
use crate::server::tcp_sender::TcpSender;
use std::io::ErrorKind;
use tracing::{debug, error, info};

const INITIAL_BYTES_LENGTH: usize = 4;

pub(crate) async fn handle_connection(sender: &mut TcpSender) -> Result<(), SystemError> {
    let mut initial_buffer = [0u8; INITIAL_BYTES_LENGTH];
    loop {
        let read_length = sender.read(&mut initial_buffer).await?;
        if read_length != INITIAL_BYTES_LENGTH {
            error!(
                "Unable to read the TCP request length, expected: {} bytes, received: {} bytes.",
                INITIAL_BYTES_LENGTH, read_length
            );
            continue;
        }

        let length = u32::from_le_bytes(initial_buffer);
        debug!("Received a TCP request, length: {}", length);
        let mut command_buffer = vec![0u8; length as usize];
        sender.read(&mut command_buffer).await?;
        debug!("Received a TCP request, payload size: {length}");
        sender.send_empty_ok_response().await?;
        debug!("Sent a TCP response.");
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
