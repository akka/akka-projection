extern crate prost_build;

fn main() {
    prost_build::compile_protos(
        &[
            "proto/RegistrationEvents.proto",
            "proto/TemperatureEvents.proto",
        ],
        &["proto/"],
    )
    .unwrap();
}
