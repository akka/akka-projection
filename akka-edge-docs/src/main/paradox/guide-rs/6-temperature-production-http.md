# HTTP temperature events

For the purposes of serving requests made by a user interface, we stream events to one in a similar
fashion to how we produce events over gRPC; at least in terms of how we source the events. There are variety
of technologies that can be used to push a stream of events to a user interface and we illustrate [Server
Sent Events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events) (SSE). 
SSE is attractive for browser-based user interfaces given its ease-of use, and because
the browser takes care of maintaining a connection with a server.

For our example, we are using [Warp](https://github.com/seanmonstar/warp) as the toolkit for serving HTTP. 
There are a number of HTTP toolkits available so you may decide on using another for other reasons. 
Our choice is here based simply on familiarity and the fact that Warp is authored by the same team that provides 
[Hyper](https://hyper.rs/), a very popular lower-level HTTP toolkit in the Rust community.

## The offset store

We have an offset store (again!). This time, things are slightly different. In our example, we assume that our
clients are stateless and will not request events given a starting offset. We can get away with this given the use
of [Streambed Logged](https://lib.rs/crates/streambed-logged) as our storage medium, and its compaction function
which keeps the number of events to something manageable. However, we still need an offset store though so we can 
correctly track offset sequence numbers. For this particular use-case, we use a `volatile_offset_store`:

Rust
:  @@snip [http_server.rs](/samples/grpc/iot-service-rs/backend/src/http_server.rs) { #offset-store }

We size the offset store with the number of entities held in memory at any one time. This ultimately results in the
amount of memory reserved for the offset store, and it will grow dynamically beyond this size if required. However,
it is good practice to size it with the maximum anticipated so that memory consumption is reasonably deterministic.

@@@ note
In all applications, constraining the number of external requests at any one time is important so that a
DDoS attack will not bring a process down. This can be best done with an application level proxy.
@@@

## The source provider

A source provider is established in the same way as when producing over gRPC, again using the same marshaller declared 
for running the entity manager @ref:[earlier in the guide](3-temperature-entity.md#serialization-and-storage):

Rust
:  @@snip [http_server.rs](/samples/grpc/iot-service-rs/backend/src/http_server.rs) { #source-provider }

## The handler

A handler function is provided to send envelopes from the commit log over a channel that we will subsequently stream
via SSE.

Rust
:  @@snip [http_server.rs](/samples/grpc/iot-service-rs/backend/src/http_server.rs) { #handler }

## The consumer

Set up the consumer task that will source events from using our in-memory offset store, source provider and handler.

Rust
:  @@snip [http_server.rs](/samples/grpc/iot-service-rs/backend/src/http_server.rs) { #consumer }

## Server Sent Events (SSE)

The final step is to consume the events produced by the consumer's handler. We do this by wrapping the receiver
end of the channel created earlier. Events are then serialized into JSON.

Rust
:  @@snip [http_server.rs](/samples/grpc/iot-service-rs/backend/src/http_server.rs) { #sse }

We use `MAX_SSE_CONNECTION_TIME` to limit the maximum amount of time that an SSE connection can be held.
Any consumer of the SSE events will automatically re-connect if they are still able
to. This circumvents keeping a TCP socket open for many minutes in the absence of
there being a connection. While producing our SSE events should relatively efficient,
we do not do it unless we really need to, and TCP is not always fast in detecting the
loss of a network connection.

## Putting it all together

The following code puts all of the above together as a route that can be served:

Rust
:  @@snip [http_server.rs](/samples/grpc/iot-service-rs/backend/src/http_server.rs) { #route }

## What's next?

* UDP observations