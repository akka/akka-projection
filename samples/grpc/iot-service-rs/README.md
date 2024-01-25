IoT service example
===

This is an example largely based on [Streambed's](https://github.com/streambed/streambed-rs/tree/main/examples/iot-service) 
and illustrates the use of streambed that writes fictitious temperature sensor observations
to a commit log. A simple HTTP API is provided to query and scan the commit log. The main change
is the use of the Akka Persistence Entity Manager in place of the `Database` struct that
the Streambed example uses, and the integration with the [iot-service](https://github.com/akka/akka-projection/tree/main/samples/grpc/iot-service-scala) of Akka projections.

Please see the [documentation on Akka Edge Rust](https://doc.akka.io/docs/akka-edge/current/guide.html) for
guidance on using this example.