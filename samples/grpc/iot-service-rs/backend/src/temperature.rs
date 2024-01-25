// Handle temperature sensor entity concerns

use akka_persistence_rs::{
    effect::{persist_event, unhandled, Effect, EffectExt},
    entity::{Context, EventSourcedBehavior},
    EntityType,
};
use akka_persistence_rs::{
    entity_manager::{self},
    Message,
};
use akka_persistence_rs_commitlog::{
    cbor::{self, Marshaller},
    CommitLogTopicAdapter,
};
use std::io;
use streambed::commit_log::Topic;
use streambed_confidant::FileSecretStore;
use streambed_logged::{compaction::NthKeyBasedRetention, FileLog};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub use iot_service_model::temperature::{Event, SecretDataValue, State};

// Declare temperature sensor entity concerns
// #commands
pub enum Command {
    Post { temperature: u32 },
    Register { secret: SecretDataValue },
}
// #commands

// #behavior-1
pub struct Behavior;
// #behavior-1

// #behavior-2
impl EventSourcedBehavior for Behavior {
    type State = State;

    type Command = Command;

    type Event = Event;

    fn for_command(
        _context: &Context,
        state: &Self::State,
        command: Self::Command,
    ) -> Box<dyn Effect<Self>> {
        match command {
            Command::Post { temperature } if !state.secret.is_empty() => {
                persist_event(Event::TemperatureRead { temperature }).boxed()
            }

            Command::Register { secret } => persist_event(Event::Registered { secret }).boxed(),

            _ => unhandled(),
        }
    }

    fn on_event(context: &Context, state: &mut Self::State, event: Self::Event) {
        state.on_event(context, event);
    }
}
// #behavior-2

// #serialization
// A namespace for our entity's events when constructing persistence ids.
pub const ENTITY_TYPE: &str = "Sensor";

pub fn marshaller(
    events_key_secret_path: String,
    secret_store: FileSecretStore,
) -> Marshaller<Event, impl Fn(&Event) -> u32, FileSecretStore> {
    let to_record_type = |event: &Event| match event {
        Event::TemperatureRead { .. } => 0,
        Event::Registered { .. } => 1,
    };

    cbor::marshaller(
        EntityType::from(ENTITY_TYPE),
        events_key_secret_path,
        secret_store,
        to_record_type,
    )
}
// #serialization

// #entity-manager
// #compaction
pub const EVENTS_TOPIC: &str = "temperature";

const MAX_HISTORY_EVENTS: usize = 10;

// This is the number of keys that "compaction" will produce in one pass. If there are more
// keys than that, then there will be another pass. You should size it to what you think
// is a reasonable number of entities for your application.
const MAX_TOPIC_COMPACTION_KEYS: usize = 1_000;
// #compaction

// The maximum number of temperature commands to retain in memory at any one time.
// Exceeding this number will back-pressure any senders of commands.
pub const MAX_COMMANDS: usize = 10;

// The maximum number of temperature entities to retain in memory at any one time.
// Exceeding this number will evict the last entity used and source any new one
// required.
pub const MAX_ENTITIES: usize = 10;

// A task that will be run to manage the temperature sensor.
pub async fn spawn(
    commit_log: FileLog,
    secret_store: FileSecretStore,
    events_key_secret_path: String,
) -> (JoinHandle<io::Result<()>>, mpsc::Sender<Message<Command>>) {
    // #compaction ...

    let events_topic = Topic::from(EVENTS_TOPIC);

    let mut task_commit_log = commit_log.clone();
    let task_events_topic = events_topic.clone();
    tokio::spawn(async move {
        task_commit_log
            .register_compaction(
                task_events_topic,
                NthKeyBasedRetention::new(MAX_TOPIC_COMPACTION_KEYS, MAX_HISTORY_EVENTS),
            )
            .await
            .unwrap();
    });
    // #compaction

    // #adapter
    let file_log_topic_adapter = CommitLogTopicAdapter::new(
        commit_log,
        marshaller(events_key_secret_path, secret_store),
        "iot-service",
        events_topic,
    );
    // #adapter

    // #run
    let (entity_manager_task, commands) =
        entity_manager::task(Behavior, file_log_topic_adapter, MAX_COMMANDS, MAX_ENTITIES);
    let handle = tokio::spawn(entity_manager_task);
    // #run

    (handle, commands)
}
// #entity-manager
