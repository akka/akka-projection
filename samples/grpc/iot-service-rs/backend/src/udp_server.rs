// Handle UDP sensor serving concerns

use akka_persistence_rs::Message;
use log::debug;
use serde::{Deserialize, Serialize};
use tokio::{net::UdpSocket, sync::mpsc};

use crate::temperature::Command;

// #run
// Messages from sensors are generally small.
const MAX_DATAGRAM_SIZE: usize = 12;

#[derive(Debug, Deserialize, Serialize)]
struct TemperatureUpdated {
    dev_addr: u32,
    temperature: u32,
}

pub async fn task(socket: UdpSocket, temperature_commands: mpsc::Sender<Message<Command>>) {
    let mut recv_buf = [0; MAX_DATAGRAM_SIZE];
    while let Ok((len, _remote_addr)) = socket.recv_from(&mut recv_buf).await {
        if let Ok(event) =
            postcard::from_bytes::<TemperatureUpdated>(&recv_buf[..len.min(MAX_DATAGRAM_SIZE)])
        {
            debug!("Posting : {:?}", event);

            let _ = temperature_commands
                .send(Message::new(
                    event.dev_addr.to_string(),
                    Command::Post {
                        temperature: event.temperature,
                    },
                ))
                .await;
        }
    }
}
// #run
