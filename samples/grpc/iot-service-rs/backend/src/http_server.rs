// Handle http serving concerns

use std::time::Duration;

use crate::temperature;
use akka_persistence_rs::EntityId;
use akka_persistence_rs_commitlog::EventEnvelope;
use akka_projection_rs::{consumer, volatile_offset_store, HandlerError};
use akka_projection_rs_commitlog::CommitLogSourceProvider;
use futures::{future, stream::StreamExt};
use streambed::commit_log::Topic;
use streambed_confidant::FileSecretStore;
use streambed_logged::FileLog;
use tokio::{sync::mpsc, time};
use tokio_stream::wrappers::ReceiverStream;
use warp::{filters::sse, Filter, Rejection, Reply};

// #route
// We use this to limit the maximum amount of time that an SSE connection can be held.
// Any consumer of the SSE events will automatically re-connect if they are still able
// to. This circumvents keeping a TCP socket open for many minutes in the absence of
// there being a connection. While producing our SSE events should relatively efficient,
// we don't do it unless we really need to, and TCP isn't great about detecting the
// loss of a network
const MAX_SSE_CONNECTION_TIME: Duration = Duration::from_secs(60);

// Declares routes to serve our HTTP interface.
pub fn routes(
    commit_log: FileLog,
    secret_store: FileSecretStore,
    events_key_secret_path: String,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let get_temperature_route = {
        warp::get()
            .and(warp::path("events"))
            .and(warp::path::param())
            .and(warp::path::end())
            .map(move |entity_id: String| {
                let entity_id = EntityId::from(entity_id);

                // #offset-store
                let offset_store = volatile_offset_store::task(temperature::MAX_ENTITIES);
                // #offset-store

                // #source-provider
                let source_provider = CommitLogSourceProvider::new(
                    commit_log.clone(),
                    temperature::marshaller(events_key_secret_path.clone(), secret_store.clone()),
                    "http-projection",
                    Topic::from(temperature::EVENTS_TOPIC),
                );
                // #source-provider

                // #handler
                let (temperature_events, temperature_events_receiver) = mpsc::channel(1);
                let handler = move |envelope: EventEnvelope<temperature::Event>| {
                    let entity_id = entity_id.clone();
                    let temperature_events = temperature_events.clone();
                    async move {
                        if envelope.persistence_id.entity_id == entity_id {
                            temperature_events
                                .send((entity_id, envelope.event))
                                .await
                                .map(|_| ())
                                .map_err(|_| HandlerError)
                        } else {
                            Ok(())
                        }
                    }
                };
                // #handler

                // #consumer
                let (consumer_task, consumer_kill_switch) =
                    consumer::task(offset_store, source_provider, handler);
                tokio::spawn(consumer_task);
                // #consumer

                // #sse
                let event_stream = ReceiverStream::new(temperature_events_receiver)
                    .map(|(entity_id, event)| {
                        let sse_event = sse::Event::default();
                        let sse_event = if let temperature::Event::Registered { .. } = event {
                            sse_event.id(entity_id)
                        } else {
                            sse_event
                        };
                        sse_event.json_data(event)
                    })
                    .take_until(async {
                        let result =
                            time::timeout(MAX_SSE_CONNECTION_TIME, future::pending::<()>()).await;
                        drop(consumer_kill_switch);
                        result
                    });

                let event_stream = sse::keep_alive().stream(event_stream);
                sse::reply(event_stream)
                // #sse
            })
    };

    let routes = get_temperature_route;

    warp::path("api").and(warp::path("temperature").and(routes))
}
// #route
