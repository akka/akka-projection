# Consuming registration events

Before a temperature entity is able to be queried for its state, or observations can be posted to it, the fictitious
temperature sensor must be registered as described previously in @ref:["Running the sample"](2-running.md).

The following section describes connecting to a remote producer of registration events, and forwarding them as registration commands to 
our temperature entity. The offset of this consumer is saved so that our service may start near where it left off in 
the case of being restarted.

@@@ note
We use the term "near" here deliberately as the guarantees are "at least once" and the last offset processed may
not have been saved - although it mostly will have been.
@@@

## The offset store

When consuming events from a remote producer, we must keep track of the offset we have processed. This is
so that we can resume near where we left off in the case of a restart. An offset store is used for this purpose, and we
will also use one when consuming from the `GrpcSourceProvider`, discussed in the next section.

Rust
:  @@snip [registration_projection.rs](/samples/grpc/iot-service-rs/backend/src/registration_projection.rs) { #offset-store }
 
We also declare the commit log to use and the expected number of distinct device registrations. 
The number of device registrations is used to determine how many entity id offsets are held in memory at any one time.
Each offset store must also have a distinct identity also, hence the `offset-store-id`.

## The gRPC source provider

We use a @extref:[`GrpcSourceProvider`](akka-edge-rs-api:akka_projection_rs_grpc/consumer/struct.GrpcSourceProvider.html) to source events from a remote producer. The remote endpoint is declared as a function
that establishes a gRPC connection. This function can be enhanced to establish the connection flexibly e.g. we can
establish a TLS-based connection or even a Unix Domain Socket based connection if desired.

Rust
:  @@snip [registration_projection.rs](/samples/grpc/iot-service-rs/backend/src/registration_projection.rs) { #source-provider }

The gRPC source provider will manage the remote connection, including exponential backoff and retries in the case 
of failure.

## The handler

Receiving events from a remote producer means little if we don't do anything with them. To this end, we declare a 
handler to forward these events on to the temperature entity via its entity manager's message channel for commands.

Rust
:  @@snip [registration_projection.rs](/samples/grpc/iot-service-rs/backend/src/registration_projection.rs) { #handler }

Note that we are only interested in `Registered` events. If our remote producer was capable of producing other types of
events then these would be filtered out.

@@@ note
We can also declare a "consumer filter" on the `GrpcSourceProvider` so that the producer avoids transmitting events
that we do not wish to consume. This is a great way to save on network bandwidth; an important concern when dealing with
the edge, particularly with wireless communications.
@@@

## Run the projection

We are now in a position to start up a projection that will use the offset store to remember the offset consumed, and
invoke the handler for each remote event received:

Rust
:  @@snip [registration_projection.rs](/samples/grpc/iot-service-rs/backend/src/registration_projection.rs) { #consume-run }

A kill switch is also provided so that its task may be terminated from elsewhere.

## Putting it all together

The following code represents putting all of the above together as a task that can be spawned:

Rust
:  @@snip [registration_projection.rs](/samples/grpc/iot-service-rs/backend/src/registration_projection.rs) { #consume }

## What's next?

* Producing temperature events
