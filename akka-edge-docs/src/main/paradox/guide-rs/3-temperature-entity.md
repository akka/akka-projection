# The temperature entity

As the other features of Akka Edge are build on top of Event Sourcing, let us start by implementing a digital twin
for temperature sensors using the same approach as the @extref[Akka Event Sourced Behavior API](akka:typed/persistence.html), but written 
using Akka Edge Rust. 

We will represent a temperature sensor as an Event Sourced entity. If you are unfamiliar with Event Sourcing, refer to the
@extref[Event Sourcing section in the Akka guide](akka-guide:concepts/event-sourcing.html) for an explanation.
The [Event Sourcing with Akka video](https://akka.io/blog/news/2020/01/07/akka-event-sourcing-video) is also a good starting point for learning about Event Sourcing.

## Commands and events

Commands are the public API of an entity that other parts of the system use to interact with it. Entity state can only
be changed by an entity's events as a result of commands. A command can request state changes, and different
events might be generated depending on the current state of the entity. A command can also be rejected if it has invalid 
input or cannot be handled due to the current state of the entity.

The Temperature sensor accepts three commands: `Post`, and `Register`, for retrieving the current state, posting
a new temperature observation, and registering a sensor respectively. When posting a new observation, a `TemperatureRead` 
is emitted and then persisted in a local event journal. Any `Post` command where a sensor has not been registered
will be ignored.

The definition of the commands looks like this:

Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/backend/src/temperature.rs) { #commands }

Temperature values in our sample are represented as integers, which is often the convention when receiving small packets 
of data from sensors. Quite often, sensors communicate over a very low bandwidth connection. The less bits to transmit, 
the more chance a sensor's data will reach its destination, particularly over Low-Powered-Wide-Area-Networks 
([LPWANs](https://en.wikipedia.org/wiki/Low-power_wide-area_network)).

The registration command conveys a `SecretDataValue` which, in our case, is a hexadecimal string that will be conveyed 
out-of-band, via Akka Edge JVM. These types of secrets are often used by sensors to produce a "session key" that both a sensor
can encrypt with, and an edge service can decrypt with. For example, 
[AES-CCM 128 bit encryption](https://en.wikipedia.org/wiki/CCM_mode) is often used to provide authentication and validation 
of the message. To keep things simple, our sample does nothing more with the secret than accepting temperature 
observations given the presence of it.

### A shared model

The definition of the events is in a separate module so that it they can be shared with a browser-based application, 
a "frontend". Sharing the data structures improves code maintenance and 
the assurance that the frontend and backend will continue to be compatible with each other. The event declarations look 
like this:
</a>

Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/model/src/temperature.rs) { #events }

As events are persisted in a local event journal, we use [Serde](https://serde.rs/) declarations to generate code for
serializing and deserializing the events to and from bytes. Events are also required to be cloneable so that they may 
be processed by various handlers e.g. when sending an event via gRPC.

## State

Up to 10 temperature observations are kept in memory. The amount of history to be
retained in memory is an application-specific concern, and will depend on the amount of memory to be reserved also considering
the number of entities held in memory at any one time.

Here is the declaration of the state structure, again in a separate module so that it can be shared between the frontend
and the backend of the sample:

Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/model/src/temperature.rs) { #state }

We use a [`VecDeque`](https://doc.rust-lang.org/std/collections/struct.VecDeque.html) for history as
we can add and remove observations from the queue in constant time. The implementation of `State` shows
how this is put to effect with a method that updates it given an event: 
 
Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/model/src/temperature.rs) { #impl-state }

This method will be called by our backend's entity behavior declaration described in the next section. It is also used
by the frontend to construct the state it requires to display to our user.
 
## Behavior 
 
The temperature entity will receive commands and produce "effects" that an "entity manager" will apply. These effects can
cause events to be emitted and then persisted to an event journal.

The structure for our temperature entity is as follows:
 
Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/backend/src/temperature.rs) { #behavior-1 }

An @extref:[`EventSourcedBehavior`](akka-edge-rs-api:akka_persistence_rs/entity/trait.EventSourcedBehavior.html) declaration associates the types used for the state, commands, and events of an entity instances. The
declaration also includes how the entity's commands are processed, and how events are applied to state. Note that it is 
impossible to update state from a command handler as it is immutable by design. Updating state only via
the event handler enables entities to be sourced from their stored events without effects.

Here is the behavior declaration of our entity, noting that the event handler is calling upon [a shared model declaration
described earlier](#a-shared-model):
 
Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/backend/src/temperature.rs) { #behavior-2 }

Each command handler declares an effect to be performed. When posting an observation in our sample, the `TemperatureRead` 
event is persisted.

## Serialization and storage

The events of the entity must be serializable because they are written to an event journal. We are using the
[Streambed Logged](https://github.com/streambed/streambed-rs/tree/main/streambed-logged) commit log as an event journal.
Streambed Logged has a particular emphasis on managing limited amounts of storage. The notion of "compaction" is important
to Streambed Logged, and ensures that only so many records of a given event type for a given entity id are retained
over time.
Akka Edge Rust provides a crate to support Streambed Logged, but we must still describe how to marshal our events to and 
from it. The following code illustrates this: 

Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/backend/src/temperature.rs) { #serialization }

We provide the marshaller as a function so that we can re-use it when projecting events (later).

With the above, Akka Edge Rust provides out-of-the-box functionality to encrypt and decrypt the records of Streamed Logged,
and use [CBOR](https://cbor.io/) for serialization.

## Compaction

Streambed Logged compaction manages the amount of storage consumed by a commit log. Compaction is used to 
manage the space occupied by storage, which is important, particularly at the edge where resources are limited.

We size compaction to the typical number of devices we expect to have in the system.
Note though that it will impact memory, so there is a trade-off. Let's suppose this
was some LoRaWAN system and that our gateway cannot handle more than 1,000 devices
being connected. We can work out that 1,000 is therefore a reasonable limit. The
overhead is small, but should be calculated and measured for a production scenario.

Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/backend/src/temperature.rs) { #compaction }

We register a compaction strategy for our topic such that when we use up
64KB of disk space (the default), we will run compaction in the background so that unwanted
events are removed. In our scenario, unwanted events can be removed when
the exceed `MAX_HISTORY_EVENTS` as we do not have a requirement to ever
return more than that.

## The file log adapter

This adapter permits us to source events from the commit log and append them to it as they are emitted by an entity.

Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/backend/src/temperature.rs) { #adapter }

## Running the entity manager

We are now ready to run the @extref:[entity manager](akka-edge-rs-api:akka_persistence_rs/entity_manager/index.html). An entity manager task handles the lifecycle 
and routing of messages per type of entity.
 
Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/backend/src/temperature.rs) { #run }

The above runs the entity manager given a behavior, the commit log adapter, and a means to receive commands via a channel.
We inform it of the `MAX_COMMANDS` that will be buffered before back-pressuring any senders. Similarly, we dimension
the "working set" of entities that are cached in memory at any one time via `MAX_ENTITIES`. A channel is returned so
that we can send commands to entities.

## Putting it all together

The following code represents putting all of the above together as a task that can be spawned:

Rust
:  @@snip [temperature.rs](/samples/grpc/iot-service-rs/backend/src/temperature.rs) { #entity-manager }


## What's next?

* Consuming registration events 
