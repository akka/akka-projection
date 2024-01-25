// Handle temperature projection concerns

use crate::proto;
use crate::temperature;
use akka_persistence_rs::EntityType;
use akka_persistence_rs_commitlog::EventEnvelope;
use akka_projection_rs::consumer;
use akka_projection_rs_commitlog::{offset_store, CommitLogSourceProvider};
use akka_projection_rs_grpc::{producer, OriginId, StreamId};
use log::info;
use streambed::commit_log::Topic;
use streambed_confidant::FileSecretStore;
use streambed_logged::FileLog;
use tokio::sync::oneshot;
use tonic::transport::{Channel, Uri};

// #task
const MAX_IN_FLIGHT: usize = 10;

// Apply sensor observations to a remote consumer.
pub fn spawn(
    commit_log: FileLog,
    event_consumer_addr: Uri,
    secret_store: FileSecretStore,
    events_key_secret_path: String,
) -> (oneshot::Sender<()>, oneshot::Sender<()>) {
    let entity_type = EntityType::from(temperature::ENTITY_TYPE);

    // #offset-store
    let stream_id = StreamId::from("temperature-events");

    let offset_store_id = stream_id.clone();
    let offset_store = offset_store::task(
        commit_log.clone(),
        temperature::MAX_ENTITIES,
        offset_store_id,
    );
    // #offset-store

    // #source-provider
    let source_provider = CommitLogSourceProvider::new(
        commit_log,
        temperature::marshaller(events_key_secret_path, secret_store),
        "iot-service-projection",
        Topic::from(temperature::EVENTS_TOPIC),
    );
    // #source-provider

    // #producer
    let consumer_connector = move || {
        let consumer_endpoint = Channel::builder(event_consumer_addr.clone());
        async move { consumer_endpoint.connect().await }
    };
    let (producer_task, producer_flow, producer_kill_switch) = producer::task(
        consumer_connector,
        OriginId::from("edge-iot-service"),
        stream_id,
        entity_type,
        MAX_IN_FLIGHT,
    );
    tokio::spawn(producer_task);
    // #producer

    // #transformer
    let transformer = |envelope: &EventEnvelope<temperature::Event>| {
        let temperature::Event::TemperatureRead { temperature } = envelope.event else {
            return None;
        };

        // #transformer
        info!(
            "Producing {} with temperature of {}",
            envelope.persistence_id, temperature
        );

        // #transformer
        let event = proto::TemperatureRead {
            temperature: temperature as i32,
        };

        Some(event)
    };
    let producer_filter = |_: &EventEnvelope<temperature::Event>| true;
    let handler = producer_flow.handler(producer_filter, transformer);
    // #transformer

    // #consumer
    let (consumer_task, consumer_kill_switch) =
        consumer::task(offset_store, source_provider, handler);
    tokio::spawn(consumer_task);
    // #consumer

    (producer_kill_switch, consumer_kill_switch)
}
// #task
