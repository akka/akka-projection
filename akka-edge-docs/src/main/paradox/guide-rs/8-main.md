# The main function

The main function brings everything together and is the entry point into the service.

## Arguments

Command line arguments are declared using [clap](https://docs.rs/clap/latest/clap/), which is popular within the Rust community.

Rust
:  @@snip [main.rs](/samples/grpc/iot-service-rs/backend/src/main.rs) { #args }

Some of the arguments used here are declared by Streambed i.e. the `cl_args` and the `ss_args` for the commit log
and secret store respectively. A reasonable set of defaults are supplied above such that the commit log and secret store
are the only arguments we need to supply to run the sample.

@@@ note
You might also use [git-version](https://docs.rs/git-version/latest/git_version/) to supply the version in the args.
@@@

## Establishing the secret store

We setup and authenticate our service with the secret store and do this by consuming a "root secret" and another 
secret to be used as a passcode for authenticating our service. These secrets are provided on the command
line as standard input to avoid being written to disk as a security consideration. The expectation is that a supervisory service, perhaps
[`systemd`](https://systemd.io/), sources and provides these secrets from a hardware-based secure enclave.

Rust
:  @@snip [main.rs](/samples/grpc/iot-service-rs/backend/src/main.rs) { #ss }

The service's root secret is used to encrypt and decrypt data with its secret store. The secret store being used here
is [Streambed Confidant](https://crates.io/crates/streambed-confidant), which provides a file-system based store.

## Generating keys

The first time a service is run it must generate some keys for the purposes of encrypting the commit log when appending
to it, and decrypting it when consuming.

Rust
:  @@snip [main.rs](/samples/grpc/iot-service-rs/backend/src/main.rs) { #keys }

## Establishing the commit log

The first instance of the commit log receives the location of where its storage should reside. This instance will be
cloned and passed to the other tasks that need it:

Rust
:  @@snip [main.rs](/samples/grpc/iot-service-rs/backend/src/main.rs) { #cl }

## Putting it all together

The remaining code spawns the other tasks covered by this guide. With exception to the argument declaration, 
here is the main function in its entirety:

Rust
:  @@snip [main.rs](/samples/grpc/iot-service-rs/backend/src/main.rs) { #run }

### Single core machines

Edge based services tend to operate on single core machines. If this is the case with yours then you should consider
establishing the executor as a single-threaded one, also reserving a number of blocking IO threads and providing a
reasonable stack size:

```rust
fn main() -> Result<(), Box<dyn Error>> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .max_blocking_threads(x)
        .thread_stack_size(2 * 1024 * 1024)
        .build()?;

    rt.block_on(main_task())
}

async fn main_task() -> Result<(), Box<dyn Error>> {
    ...
}
```
