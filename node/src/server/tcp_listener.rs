use crate::clusters::cluster::Cluster;
use crate::connection::handler::ConnectionHandler;
use crate::server::command_handler;
use sdk::commands::command::Command;
use sdk::error::SystemError;
use std::rc::Rc;
use tracing::{debug, error, warn};

const INITIAL_BYTES_LENGTH: usize = 8;

pub async fn listen(
    handler: &mut ConnectionHandler,
    cluster: Rc<Cluster>,
) -> Result<(), SystemError> {
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

        let command = if length == 0 {
            Command::from_bytes(code, &[])
        } else {
            let mut payload = vec![0u8; length as usize];
            (_, payload) = handler.read(payload).await?;
            Command::from_bytes(code, &payload)
        };

        if command.is_err() {
            error!("Unable to parse the TCP request.");
            handler
                .send_error_response(SystemError::InvalidRequest)
                .await?;
            continue;
        }

        let command = command.unwrap();
        if let Err(error) = command_handler::handle(handler, &command, &cluster).await {
            error!("Unable to handle the TCP request.");
            if let SystemError::UnhealthyCluster = &error {
                warn!("Cluster is not healthy, unable to handle the TCP request.");
            }
            handler.send_error_response(error).await?;
        }
    }
}
