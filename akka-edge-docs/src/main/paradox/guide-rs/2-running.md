# Running the sample

The sample Akka Edge Rust project is a fairly complete example of how a service can be written for the edge, and includes encryption.
Encryption should normally be applied to data storage and transmission. For simplicity, we apply encryption to storage 
data, but not http and UDP.

## Running

The complete sample can be downloaded from [iot-service.zip](../attachments/iot-service.zip) 

Then unzip and navigate to the `iot-service-rs` project:

```
cd iot-service-rs
```

To run via cargo:

@@@ note
If you have previously run the Rust service then run the following to start again from scratch:

```
rm -rf /tmp/iot-service/var/lib
```
@@@

```
mkdir -p /tmp/iot-service/var/lib/confidant
chmod 700 /tmp/iot-service/var/lib/confidant
mkdir -p /tmp/iot-service/var/lib/logged
echo -n "01234567890123456789012345678912some-secret-id" | \
RUST_LOG=info cargo run --manifest-path=backend/Cargo.toml -- \
  --cl-root-path=/tmp/iot-service/var/lib/logged \
  --ss-role-id="iot-service" \
  --ss-root-path=/tmp/iot-service/var/lib/confidant
```

@@@ note
If you would like to understand what the above command line parameters refer to, or learn about more options,
then you can also type:

```
cargo run --manifest-path=backend/Cargo.toml -- --help
```
@@@

We must first register the device ids that we wish to receive data for. This is a form
of authentication where, in the real-world, a shared key between the device and service
would be conveyed. That key would then be used to encrypt data. We simply use the key
as a registration mechanism and do not accept data for devices where we have no key.

Let's first query for a sensor's data. Open another  terminal window and type the following:

```
curl -v "127.0.0.1:8080/api/temperature/events/1"
```

...it will return an empty stream as we have no sensors yet.

So, let's now provision one. To do this, we must start up the JVM-based iot-service. From a new terminal window navigate
to the iot-service project from within the repository:

Scala
:   
```
cd iot-service-scala
```

Java
:   
```
cd iot-service-java
```

@@@ note
If you have previously run the JVM iot-service then run the following to start again from scratch:

```
docker rm -f postgres_db 
```
@@@

Please follow steps 1 and 2 at the JVM @scala[`iot-service-scala/README.md`]@java[`iot-service-java/README.md`]. Once done, provision a sensor back in the terminal window where you previously issued `curl`:

```
grpcurl \
  -d '{"sensor_entity_id":"1", "secret":"foo"}' \
  -plaintext \
  127.0.0.1:8101 \
  iot.registration.RegistrationService.Register
```

You should now be able to query for the current state of a temperature sensor, although
they'll be no observations recorded for it yet, so it will still be an empty stream. However,
this time it is waiting on events.

```
curl -v "127.0.0.1:8080/api/temperature/events/1"
```

From another terminal, let's now post database events to the UDP socket so that the sensor has observations. Note that
we're using Postcard to deserialize binary data. Postcard uses variable length
integers where the top bit, when set, indicates that the next byte also contains
data. See [Postcard](https://docs.rs/postcard/latest/postcard/) for more details.

```
echo -n -e "\x01\x02" | nc -w0 127.0.0.1 -u 8081
```

You should see an `INFO` log in the JVM iot-service indicating that the post has been received. You will also see events
appearing from the above curl command.

Back over in the JVM iot-service, you should also see these temperature observations
appear in its log, and you can retrieve the latest observation with:

```
grpcurl \
  -d '{"sensor_entity_id":"1"}' \
  -plaintext \
  127.0.0.1:8101 \
  iot.temperature.SensorTwinService.GetTemperature
```

Now let's run the user interface. Please follow [Yew's getting started guide](https://yew.rs/docs/getting-started/introduction)
to install `trunk` along with the Rust Webassembly target. Then, from the `iot-service-rs` project:

```
(cd frontend && trunk serve)
```

.. and then navigate to http://localhost:8081/. Looking up entity `1` will display the temperature observations
that have been sent over UDP. 

@@@ note
If you encounter any issues using `trunk` then it may be because you already have a version of the `sass` compiler
installed on your system. Version `1.54.9` of the sass compiler is required at a minimum.
@@@

To see an observation display in near-real-time, keep the browser window visible and 
then update the temperature observation with another value, in this case 3 degrees:

```
echo -n -e "\x01\x03" | nc -w0 127.0.0.1 -u 8081
```

## What's next?

* The temperature entity
