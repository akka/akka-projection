// Handle registration projection concerns

use akka_persistence_rs::Message;
use akka_projection_rs::{consumer, HandlerError};
use akka_projection_rs_commitlog::offset_store;
use akka_projection_rs_grpc::{consumer::GrpcSourceProvider, EventEnvelope, StreamId};
use log::info;
use streambed_logged::FileLog;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;
use tonic::transport::Uri;

use crate::registration;
use crate::temperature;

// #consume

const EXPECTED_DISTINCT_REGISTRATIONS: usize = 1000;

// Apply sensor registrations to the temperature sensor entity.

pub fn spawn(
    commit_log: FileLog,
    event_producer_addr: Uri,
    temperature: mpsc::Sender<Message<temperature::Command>>,
) -> oneshot::Sender<()> {
    // #offset-store
    let stream_id = StreamId::from("registration-events");

    let offset_store_id = stream_id.clone();
    let offset_store =
        offset_store::task(commit_log, EXPECTED_DISTINCT_REGISTRATIONS, offset_store_id);
    // #offset-store

    // #source-provider
    let source_provider = GrpcSourceProvider::new(
        move || {
            let event_producer = Channel::builder(event_producer_addr.clone());
            async move { event_producer.connect().await }
        },
        stream_id,
    );
    // #source-provider

    // #handler
    let handler = move |envelope: EventEnvelope<registration::Registered>| {
        let temperature = temperature.clone();
        async move {
            let (entity_id, secret) = {
                let secret = {
                    let registration::Registered {
                        secret: Some(secret),
                        ..
                    } = envelope.event
                    else {
                        return Ok(());
                    };
                    secret.value.into()
                };
                (envelope.persistence_id.entity_id, secret)
            };

            // #handler
            info!("Consuming {entity_id}");

            // #handler
            temperature
                .send(Message::new(
                    entity_id,
                    temperature::Command::Register { secret },
                ))
                .await
                .map(|_| ())
                .map_err(|_| HandlerError)
        }
    };
    // #handler

    // #consume-run
    let (consumer_task, consumer_kill_switch) =
        consumer::task(offset_store, source_provider, handler);
    tokio::spawn(consumer_task);
    // #consume-run

    consumer_kill_switch
}

// #consume
