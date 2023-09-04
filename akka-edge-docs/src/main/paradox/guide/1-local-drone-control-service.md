# Local Drone Control Service

As the other features of Akka Edge are build on top of Event Sourcing, let us start by implementing a digital twin
to drones using the @extref[Akka Event Sourced Behavior API](akka:typed/persistence.html). When this first step is completed, the drones
will be able to report their location and users inspect the current location of a drone connected to the local 
control center PoP.

We will build the drone as an Event Sourced entity, if you are unfamiliar with Event Sourcing, refer to the
@extref[Event Sourcing section in the Akka guide](akka-guide:concepts/event-sourcing.html) for an explanation.
The [Event Sourcing with Akka video](https://akka.io/blog/news/2020/01/07/akka-event-sourcing-video) is also a good starting point for learning Event Sourcing.

## Implementing a Drone digital twin

### Commands and events

Commands are the public API of an entity that other parts of the system use to interact with it. Entity state can only be changed by commands. The results of commands are emitted as events. A command can request state changes, and different events might be generated depending on the current state of the entity. A command can also be rejected if it has invalid input or can’t be handled by the current state of the entity.

The Drone only accepts two commands: `ReportLocation` and `GetLocation`. When the reported location changes it always persists
a `PositionUpdated` event, but additionally, whenever the position means it changed place on a more coarse grained grid,
it also emits a `CoarseGrainedLocationChanged` event. 

We will revisit the reason for the coarse grained event later in this guide. 

The definition of the commands and events look like this:

Scala
:  @@snip [Drone.scala](/samples/grpc/local-drone-control-scala/src/main/scala/local/drones/Drone.scala) { #commands #events }

Java
:  @@snip [Drone.java](/samples/grpc/local-drone-control-java/src/main/java/local/drones/Drone.java) { #commands #events }

### State

When the location is reported it is kept as a `currentState`, additionally the 100 previous reported locations are kept
in a list. 

The list of historical locations is not currently used for anything but is here to show that an entity could keep a time window of 
fine-grained information to make local decisions at a detail level that would be impractical and maybe not even interesting
to report to a central cloud service.

Scala
:  @@snip [Drone.scala](/samples/grpc/local-drone-control-scala/src/main/scala/local/drones/Drone.scala) { #state }

Java
:  @@snip [Drone.java](/samples/grpc/local-drone-control-java/src/main/java/local/drones/Drone.java) { #state }


### Command handler

The Drone entity will receive commands that report when the Drone changes location. We will implement a command handler to process these commands and emit a reply.

The command handler for the Drone looks like this:

Scala
:  @@snip [Drone.scala](/samples/grpc/local-drone-control-scala/src/main/scala/local/drones/Drone.scala) { #commandHandler }

Java
:  @@snip [Drone.java](/samples/grpc/local-drone-control-java/src/main/java/local/drones/Drone.java) { #commandHandler }

Note how the handler de-duplicates reports of the same location, immediately replying with an acknowledgement without
persisting any change.

In addition to storing the `PositionUpdated` event, the command handler also calculates a coarse grained location, and
persists a `CoarseGrainedLocationChanged` event as well, if it did change from the previous coarse grained location.

### Event handler

From commands, the entity creates events that represent state changes. Aligning with the command handler above, the entity’s event handler reacts to events and updates the state. The events are continuously persisted to the Event Journal datastore, while the entity state is kept in memory. Other parts of the application may listen to the events. In case of a restart, the entity recovers its latest state by replaying the events from the Event Journal.

The event handler only reacts to the `PositionUpdated` event and ignores the `CoarseGrainedLocationChanged` as the coarse
grained location can be calculated from the more fine-grained position coordinates: 

Scala
:  @@snip [Drone.scala](/samples/grpc/local-drone-control-scala/src/main/scala/local/drones/Drone.scala) { #eventHandler }

Java
:  @@snip [Drone.java](/samples/grpc/local-drone-control-java/src/main/java/local/drones/Drone.java) { #eventHandler }

### Serialization

The state and events of the entity must be serializable because they are written to the datastore, if the local drone control needs to scale out across several nodes to handle traffic, the commands would also be sent between nodes within the Akka cluster. The sample project includes built-in CBOR serialization using the @extref[Akka Serialization Jackson module](akka:serialization-jackson.html). This section describes how serialization is implemented. You do not need to do anything specific to take advantage of CBOR, but this section explains how it is included.

The state, commands and events are marked as CborSerializable which is configured to use the built-in CBOR serialization. The sample project includes this marker interface CborSerializable:

Scala
:  @@snip [CborSerializable.scala](/samples/grpc/local-drone-control-scala/src/main/scala/local/drones/CborSerializable.scala) { }

Java
:  @@snip [CborSerializable.java](/samples/grpc/local-drone-control-java/src/main/java/local/drones/CborSerializable.java) { }


Configuration in the application configuration to select the serializer:

Scala
:  @@snip [serialization.conf](/samples/grpc/local-drone-control-scala/src/main/resources/serialization.conf) { }

Java
:  @@snip [serialization.conf](/samples/grpc/local-drone-control-java/src/main/resources/serialization.conf) { }

### Journal storage

In this sample we use Akka Persistence R2DBC with the H2 in-process database, with a file backe storage. H2 requires no
additional external database service so can be convenient for both development and production usage where only a single
node interacts with the journal and overhead needs to be kept low. 

It is of course also possible to instead use a separate standalone database such as for example PostgreSQL.

Config to use H2 looks like this:

Scala
:  @@snip [persistence.conf](/samples/grpc/local-drone-control-scala/src/main/resources/persistence.conf) { }

Java
:  @@snip [persistence.conf](/samples/grpc/local-drone-control-java/src/main/resources/persistence.conf) { }

In addition to the configuration, the following additional dependencies are needed in the project build:

@@dependency [sbt,Maven,Gradle] {
group=com.h2database
artifact=h2
version=$h2.version$
group2=io.r2dbc
artifact2=r2dbc-h2
version2=$r2dbc-h2.version$
}

### gRPC Service API for the drone communication

To allow drones to actually use the service we need a public API reachable over the network. For this we will use @extref[Akka gRPC](akka-grpc:)
giving us a type safe, efficient protocol that allows clients to be written in many languages.

The service descriptor for the API is defined in protobuf, it implements the report command that entity accepts but not
one matching the get location command:

Scala
:  @@snip [local.drones.drone_api.proto](/samples/grpc/local-drone-control-scala/src/main/protobuf/local/drones/drone_api.proto) { }

Java
:  @@snip [local.drones.drone_api.proto](/samples/grpc/local-drone-control-java/src/main/protobuf/local/drones/drone_api.proto) { }

When compiling the project the Akka gRPC @scala[sbt]@java[maven] plugin generates a service interface for us to implement.
Our implementation of it interacts with the entity:

Scala
:  @@snip [DroneServiceImpl.scala](/samples/grpc/local-drone-control-scala/src/main/scala/local/drones/DroneServiceImpl.scala) { }

Java
:  @@snip [DroneServiceImpl.java](/samples/grpc/local-drone-control-java/src/main/java/local/drones/DroneServiceImpl.java) { }

Finally, we need to start the HTTP server, making service implementation available for calls from drones:

Scala
:  @@snip [LocalDroneControlServer.scala](/samples/grpc/local-drone-control-scala/src/main/scala/local/drones/LocalDroneControlServer.scala) { #bind }

Java
:  @@snip [LocalDroneControlServer.java](/samples/grpc/local-drone-control-java/src/main/java/local/drones/LocalDroneControlServer.java) { #bind }

The Akka HTTP server must be running with HTTP/2 to serve gRPC, this is done through config:

Scala
:  @@snip [grpc.conf](/samples/grpc/local-drone-control-scala/src/main/resources/grpc.conf) { #http2 }

Java
:  @@snip [grpc.conf](/samples/grpc/local-drone-control-java/src/main/resources/grpc.conf) { #http2 }


// FIXME maybe this is enough and the next should be a separate page?

## Coarse grained location aggregation and publishing

We have already seen the additional `CoarseGrainedLocationChanged` event a few times, we will also publish these aggregate
events upstream to a cloud service that can keep a rough overview of where all drones are without needing to handle the
global load of detailed and frequent updates from all drones.

Normally for Akka gRPC projections the consumer initiates the connection, but in edge scenarios it might be problematic
because of firewalls not allowing the cloud to connect to each PoP. The normal consumer initiated connections also means
that all producers must be known up front by the consumer. 

To solve this the local control center push events to the cloud using @extref[Akka gRPC projection with producer push](akka-projection:grpc-producer-push.html)
which means the control center will initiate the connection.

The actual pushing of events is implemented as a single actor behavior, if partitioning is needed for scaling that is also possible
by letting multiple actors handle partitions of the entire stream of events from local drones.

Scala
:  @@snip [DroneEvents.scala](/samples/grpc/local-drone-control-scala/src/main/scala/local/drones/DroneEvents.scala) { }

Java
:  @@snip [DroneEvents.java](/samples/grpc/local-drone-control-java/src/main/java/local/drones/DroneEvents.java) { }

Two important things to note:

1. A producer filter is applied to only push `CoarseGrainedLocationChanged` and not the fine-grained `PositionUpdated` events.
2. The internal domain representation of `CoarseGrainedLocationChanged` is transformed into an explicit public protocol
   protobuf message `local.drones.proto.CoarseDroneLocation` message, for loose coupling between consumer and producer and
   easier evolution over time without breaking wire compatibility.
3. The service defines a "location name" which is a unique identifier of the PoP in the format `country/city/part-of-city`,
   it is used as `originId` for the producer push stream, identifying where the stream of events come from.


## Running the sample

The complete sample can be downloaded from github, but note that it also includes the next steps of the guide:

* Java: https://github.com/akka/akka-projection/tree/main/samples/grpc/local-drone-control-service-java
* Scala: https://github.com/akka/akka-projection/tree/main/samples/grpc/local-drone-control-service-scala

Note that since we have not implemented the consumer for the coarse grained location updates yet, the event push will
keep trying to connect to an upstream service, fail and report that in the logs.

@@@ div { .group-scala }

To start the sample:

```shell
sbt run
```

@@@

@@@ div { .group-java }

```shell
mvn compile exec:exec
```

@@@

Try it with [grpcurl](https://github.com/fullstorydev/grpcurl):

```shell
# report the location for a drone with id drone1 
grpgrpcurl -d '{"drone_id":"drone1", "coordinates": {"longitude": 18.07125, "latitude": 59.31834}, "altitude": 5}' -plaintext 127.0.0.1:8080 local.drones.DroneService.ReportLocation
```

## What's next?

* Consuming the published coarse grained drone locations in a cloud service 