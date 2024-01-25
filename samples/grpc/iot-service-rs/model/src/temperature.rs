// Handle temperature sensor state concerns

use std::collections::VecDeque;

use akka_persistence_rs::entity::Context;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

// Declare the state and how it is to be sourced from events

// #state
const MAX_HISTORY_EVENTS: usize = 10;

#[derive(Default)]
pub struct State {
    pub history: VecDeque<u32>,
    pub secret: SecretDataValue,
}
// #state

// #impl-state
impl State {
    pub fn on_event(&mut self, _context: &Context, event: Event) {
        match event {
            Event::Registered { secret } => {
                self.secret = secret;
            }
            Event::TemperatureRead { temperature } => {
                if self.history.len() == MAX_HISTORY_EVENTS {
                    self.history.pop_front();
                }
                self.history.push_back(temperature);
            }
        }
    }
}
// #impl-state

pub type SecretDataValue = SmolStr;

// #events
#[derive(Clone, Deserialize, Serialize)]
pub enum Event {
    Registered { secret: SecretDataValue },
    TemperatureRead { temperature: u32 },
}
// #events
