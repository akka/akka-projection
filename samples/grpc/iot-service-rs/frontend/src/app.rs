use akka_persistence_rs::{entity::Context as EntityContext, EntityId};
use tokio::sync::{broadcast, mpsc};
use web_sys::HtmlInputElement;
use yew::{platform, prelude::*};

use crate::temperature;

#[derive(Default, Properties, PartialEq)]
pub struct Props;

pub struct App {
    entity_id: EntityId,
    temperature_query: mpsc::Sender<EntityId>,
    temperature: temperature::State,
}

const MAX_TEMPERATURE_QUERIES: usize = 10;
const MAX_TEMPERATURE_EVENTS: usize = 10;

pub enum Message {
    EntityIdChanged {
        entity_id: EntityId,
    },
    Submit,
    UpdateTemperature {
        envelope: (EntityId, temperature::Event),
    },
}

impl Component for App {
    type Message = Message;

    type Properties = Props;

    fn create(ctx: &Context<Self>) -> Self {
        let (temperature_events, mut temperature_event_receiver) =
            broadcast::channel(MAX_TEMPERATURE_EVENTS);

        let (temperature_query, temperature_query_receiver) =
            mpsc::channel(MAX_TEMPERATURE_QUERIES);

        // Spawn a task that will take care of querying for temperature
        // events.

        platform::spawn_local(temperature::task(
            temperature_query_receiver,
            temperature_events,
        ));

        // Spawn a task that will receive the temperature events from the
        // querying and map them to events that can be received by this
        // component. We could make the query task aware of components
        // and manage this mapping, but it is cleaner to keep knowledge
        // of components out of the query task i.e. the query task is
        // not concerned with what is subscribing its its events.

        let task_link = ctx.link().clone();
        platform::spawn_local(async move {
            loop {
                let result = temperature_event_receiver.recv().await;
                match result {
                    Ok(envelope) => task_link.send_message(Message::UpdateTemperature { envelope }),
                    Err(broadcast::error::RecvError::Lagged(_)) => (),
                    Err(_) => break,
                }
            }
        });

        Self {
            entity_id: EntityId::from(""),
            temperature_query,
            temperature: temperature::State::default(),
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Self::Message) -> bool {
        match msg {
            Message::EntityIdChanged { entity_id } => {
                self.entity_id = entity_id;
                false
            }

            Message::Submit => {
                self.temperature = temperature::State::default();

                let task_query = self.temperature_query.clone();
                let task_entity_id = self.entity_id.clone();
                ctx.link().send_future_batch(async move {
                    let _ = task_query.send(task_entity_id).await;
                    vec![]
                });

                true
            }

            Message::UpdateTemperature {
                envelope: (entity_id, event),
            } => {
                if entity_id == self.entity_id {
                    self.temperature.on_event(
                        &EntityContext {
                            entity_id: &entity_id,
                        },
                        event,
                    );
                    true
                } else {
                    false
                }
            }
        }
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let on_entity_id_change = ctx.link().callback(|e: InputEvent| {
            let input: HtmlInputElement = e.target_unchecked_into();
            Message::EntityIdChanged {
                entity_id: EntityId::from(input.value()),
            }
        });

        let onsubmit = ctx.link().callback(|e: SubmitEvent| {
            e.prevent_default();
            Message::Submit
        });

        let temperature = self
            .temperature
            .history
            .iter()
            .last()
            .map(|t| format!("{t}C"))
            .unwrap_or(String::from("-"));

        html! {
            <main>
                <img class="logo" src="akka_full_color.svg" alt="Akka logo" />
                <h1>{ "Temperature observations" }</h1>
                <section>
                    <form onsubmit={onsubmit}>
                        <label for="entity-id">{"Entity Id"}</label>
                        <input type="number" id="entity-id" oninput={on_entity_id_change}/>
                        <input type="submit" value="Observe" />
                    </form>
                    <form>
                        <label for="temperature">{"Temperature"}</label>
                        <input type="text" id="temperature" readonly=true value={temperature} />
                    </form>
                </section>
            </main>
        }
    }
}
