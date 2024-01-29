# Producing temperature events

Once registered, the temperature entity can receive observations. These observations are then produced to a remote consumer,
and this page describes how we produce events over gRPC. We can also produce events over HTTP and do so 
@ref:[later in the guide](6-temperature-production-http.md).

An interesting aspect of connectivity from the edge is that it typically connects to the cloud, and not the other way
round. This is largely 
due to network traffic restrictions where the cloud cannot "see in" to the edge, but the edge can "reach out". Akka
provides a "projection producer" for the purposes of establishing a connection and then producing events.

## The offset store

We wish to avoid producing events that we have produced before. As with consuming registration events, we establish
an offset store to keep track of events that we produced.

Rust
:  @@snip [temperature_production.rs](/samples/grpc/iot-service-rs/backend/src/temperature_production.rs) { #offset-store }
 
We also declare the commit log to use and the expected number of distinct entities for sizing memory usage. 
As with consuming registrations, the number of temperature productions is used to determine how many entity id offsets 
are held in memory at any one time. Each offset store must also have a distinct identity, hence the `offset-store-id`.

## The source provider

We establish our source of events as a commit log using a source provider with the same marshaller declared for 
running the entity manager @ref:[earlier in the guide](3-temperature-entity.md#serialization-and-storage):

Rust
:  @@snip [temperature_production.rs](/samples/grpc/iot-service-rs/backend/src/temperature_production.rs) { #source-provider }

## The producer

A "sink" of envelopes is established that forward on to a remote consumer via a @extref:[producer flow](akka-edge-rs-api:akka_projection_rs_grpc/producer/index.html). The flow will be used 
to bridge between source of events and this sink.

A great deal of flexibility is provided in terms of expressing how the remote consumer's endpoint is established via the
`consumer_connector`.
Quite often, this will be the establishment of an HTTP TLS endpoint, but it can also be plaintext (as it is in the example), or
even Unix Domain Sockets.

Rust
:  @@snip [temperature_production.rs](/samples/grpc/iot-service-rs/backend/src/temperature_production.rs) { #producer }

`MAX_IN_FLIGHT` represents the maximum number of publications of an event waiting for an acknowledgement that will 
buffer before back-pressuring the producer.

A producer is independent from the source of events in that it produces at a different rate to what is sourced.
Consequently, the producer is spawned into its own task, and it can buffer the source of events across the flow.

## The transformer and handler

We must declare how our events are to be transformed into their gRPC form, and then obtain a handler from any required 
filtering.

Rust
:  @@snip [temperature_production.rs](/samples/grpc/iot-service-rs/backend/src/temperature_production.rs) { #transformer }

## The consumer

At this stage, we have a means to produce events over a gRPC producer. The final step is to start up a projection that 
uses the flow to source events from the commit log. The projection is able to resume from 
where we left off given the offset store.

Rust
:  @@snip [temperature_production.rs](/samples/grpc/iot-service-rs/backend/src/temperature_production.rs) { #consumer }

## Putting it all together

The following code puts all of the above together as a task that can be spawned:

Rust
:  @@snip [temperature_production.rs](/samples/grpc/iot-service-rs/backend/src/temperature_production.rs) { #task }

## What's next?

* HTTP temperature events
