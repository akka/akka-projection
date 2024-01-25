# UDP observations

To receive observations we use [UDP](https://en.wikipedia.org/wiki/User_Datagram_Protocol). 
Sensors can use a variety of transports and they tend to be "best-effort", sometimes
repeating events to improve their chances of reaching a destination. The following code illustrates binding to 
a UDP interface, receiving and deserializing a binary packet and then sending a `Post` command to the temperature entity.

Rust
:  @@snip [http_server.rs](/samples/grpc/iot-service-rs/backend/src/udp_server.rs) { #run }

We are using [Postcard](https://crates.io/crates/postcard) for deserializing messages. Postcard has highly reasonable 
performance and works well on embedded devices that also use Rust.

## What's next?

* The main function