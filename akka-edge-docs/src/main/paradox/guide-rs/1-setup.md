# Getting started

## Installing Rust and its tooling

Akka Edge Rust requires a minimum of Rust version 1.70.0 stable (June 2023), but the most recent stable version of Rust is recommended.

To install Rust, follow [the official instructions](https://www.rust-lang.org/learn/get-started).

[Trunk](https://trunkrs.dev/) is used to build and package our frontend example for WebAssembly. Please follow its getting-started
instructions.

Finally, the following command will [allow the Rust compiler to target WebAssembly](https://www.rust-lang.org/tools/install) for the browser:

```
rustup target add wasm32-unknown-unknown
```

## Installing the protobuf compiler

A recent version of the protobuf compiler is required (version 23 onwards). You can verify any current version by:

```
protoc --version
```

Please visit the [official gRPC site](https://grpc.io/docs/protoc-installation/) for more information on installing
Protobuf.

## What's next?

* Running the sample
