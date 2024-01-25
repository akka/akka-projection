mod http_server;
mod proto;
mod registration_projection;
mod temperature;
mod temperature_production;
mod udp_server;

use clap::Parser;
use log::info;
use proto as registration;
use rand::RngCore;
use std::{collections::HashMap, error::Error, net::SocketAddr};
use streambed::secret_store::{SecretData, SecretStore};
use streambed_confidant::{args::SsArgs, FileSecretStore};
use streambed_logged::{args::CommitLogArgs, FileLog};
use tokio::net::UdpSocket;
use tonic::transport::Uri;

// #args
#[derive(Parser, Debug)]
#[clap(author, about, long_about = None)]
struct Args {
    // Logged commit log args
    #[clap(flatten)]
    cl_args: CommitLogArgs,

    // A socket address for connecting to a GRPC event consuming
    // service for temperature observations.
    #[clap(env, long, default_value = "http://127.0.0.1:8101")]
    event_consumer_addr: Uri,

    // A socket address for connecting to a GRPC event producing
    // service for registrations.
    #[clap(env, long, default_value = "http://127.0.0.1:8101")]
    event_producer_addr: Uri,

    // A socket address for serving our HTTP web service requests.
    #[clap(env, long, default_value = "127.0.0.1:8080")]
    http_addr: SocketAddr,

    // Logged commit log args
    #[clap(flatten)]
    ss_args: SsArgs,

    // A socket address for receiving telemetry from our fictitious
    // sensor.
    #[clap(env, long, default_value = "127.0.0.1:8081")]
    udp_addr: SocketAddr,
}
// #args

// #run
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder().format_timestamp_millis().init();

    // #ss
    let ss = {
        let line = streambed::read_line(std::io::stdin()).unwrap();
        assert!(!line.is_empty(), "Failed to source a line from stdin");
        let (root_secret, ss_secret_id) = line.split_at(32);
        let root_secret = hex::decode(root_secret).unwrap();

        let ss = FileSecretStore::new(
            args.ss_args.ss_root_path,
            &root_secret.try_into().unwrap(),
            args.ss_args.ss_unauthorized_timeout.into(),
            args.ss_args.ss_max_secrets,
            args.ss_args.ss_ttl_field.as_deref(),
        );

        ss.approle_auth(&args.ss_args.ss_role_id, ss_secret_id)
            .await
            .unwrap();

        ss
    };
    // #ss

    // #keys
    let temperature_events_key_secret_path =
        format!("{}/secrets.temperature-events.key", args.ss_args.ss_ns);

    if let Ok(None) = ss.get_secret(&temperature_events_key_secret_path).await {
        // If we can't write this initial secret then all bets are off
        let mut key = vec![0; 16];
        rand::thread_rng().fill_bytes(&mut key);
        let data = HashMap::from([("value".to_string(), hex::encode(key))]);
        ss.create_secret(&temperature_events_key_secret_path, SecretData { data })
            .await
            .unwrap();
    }
    // #keys

    // #cl
    let cl = FileLog::new(args.cl_args.cl_root_path.clone());
    // #cl

    // Establish the task and command sender for the temperature entity
    let (temperature_entity_manager, temperature_commands) = temperature::spawn(
        cl.clone(),
        ss.clone(),
        temperature_events_key_secret_path.clone(),
    )
    .await;

    // Start up a task to manage registration projections
    let _registration_projection_kill_switch = registration_projection::spawn(
        cl.clone(),
        args.event_producer_addr,
        temperature_commands.clone(),
    );

    // Start up a task to manage temperature productions
    let _temperature_projection_kill_switch = temperature_production::spawn(
        cl.clone(),
        args.event_consumer_addr,
        ss.clone(),
        temperature_events_key_secret_path.clone(),
    );

    // Start up the http service
    let routes = http_server::routes(cl, ss, temperature_events_key_secret_path);
    tokio::spawn(warp::serve(routes).run(args.http_addr));
    info!("HTTP listening on {}", args.http_addr);

    // Start up the UDP service
    let socket = UdpSocket::bind(args.udp_addr).await?;
    tokio::spawn(udp_server::task(socket, temperature_commands));
    info!("UDP listening on {}", args.udp_addr);

    // All things started. Wait for the entity manager to complete.

    info!("IoT service ready");
    let _ = temperature_entity_manager.await?;

    // If we get here then we are shutting down. Any other task,
    // such as the projection one, will stop automatically given
    // that its sender will be dropped.

    Ok(())
}
// #run
