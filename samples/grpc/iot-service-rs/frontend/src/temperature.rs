// Declare and query the temperature event source

use std::time::Duration;

use akka_persistence_rs::EntityId;
use gloo_net::eventsource::{futures::EventSource, EventSourceError};
use js_sys::Math;
use log::{debug, error, warn};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;

pub use iot_service_model::temperature::{Event, State};
use yew::platform;

// Execute the event source query for commanded entity ids

pub async fn task(
    mut query_receiver: mpsc::Receiver<EntityId>,
    events: broadcast::Sender<(EntityId, Event)>,
) {
    if let Some(mut entity_id) = query_receiver.recv().await {
        'outer: loop {
            let url: &str = &format!("/api/temperature/events/{entity_id}");
            let mut temperature_es = EventSource::new(url).unwrap();
            let mut temperature_events = temperature_es.subscribe("message").unwrap();

            loop {
                tokio::select! {
                    Some(event) = temperature_events.next() => {
                        match event {
                            Ok((_, message)) => {
                                let data: Option<String> = message.data().as_string();

                                if let Some(data) = data {
                                    match serde_json::from_str::<Event>(&data) {
                                        Ok(event) => {
                                            let event_entity_id = EntityId::from(message.last_event_id());
                                            let _ = events.send((event_entity_id, event));
                                        }
                                        Err(e) => {
                                            error!("Failed to parse event: {}", e);
                                        }
                                    };
                                } else {
                                    warn!("Received event with no data");
                                }
                            }

                            Err(EventSourceError::ConnectionError) => {
                                debug!("Connection error - waiting before retry");
                                let timeout_ms = (Math::random() * 4.0) as u64 * 1000 + 1000;
                                platform::time::sleep(Duration::from_millis(timeout_ms)).await;
                                break;
                            }

                            Err(_) => (),
                        }
                    }

                    Some(next_entity_id) = query_receiver.recv() => {
                        entity_id = next_entity_id;
                        break
                    }

                    else => break 'outer,
                }
            }
        }
    }
}
