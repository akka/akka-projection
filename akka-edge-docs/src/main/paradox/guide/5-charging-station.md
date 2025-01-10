# Drone Charging Station

To showcase active-active replication between edge and cloud we'll now add a @extref[Replicated Event Sourced Entity](akka-projection:grpc-replicated-event-sourcing-transport.html)
in the form of a charging station. The charging stations are created with a location id placing them in one of the 
local-drone-control edge services where the entity is replicated. Drones in that location can request to charge in the
charging station, and be charged if there is a free charging slot.

## Implementing the charging station entity

### Commands and events

The charging station accepts three different commands from the outside `Create` to initialize a charging station, 
`StartCharging` to start a charging session for a drone if possible and `GetState` to query the station for its current 
state. There is also a private `CompleteCharging` command that only the entity can create.

The `Create` command leads to a `Created` event which is persisted and initialized the charging station.

When a slot is free and a drone requests charging a `ChargingStarted` event is persisted and once charging a drone has
completed a `ChargingCompleted` event is persisted:

Scala
:  @@snip [ChargingStation.scala](/samples/grpc/local-drone-control-scala/src/main/scala/charging/ChargingStation.scala) { #commands #events }

Java
:  @@snip [ChargingStation.java](/samples/grpc/local-drone-control-java/src/main/java/charging/ChargingStation.java) { #commands #events }

### State

The state of the charging station starts as @java[`null`]@scala[`None`] and requires a `Create` message for the station
to be initialized with a @java[`State`]@scala[`Some(State)`].

The `State` contains the number of charging slots that can concurrently charge drones and a set of currently charging drones.

The state also contains a location id identifying where it is, matching the location id structure of
the local-drone-control service. This is needed so that the station can be replicated only to the edge location where
it is located.

Scala
:  @@snip [ChargingStation.scala](/samples/grpc/local-drone-control-scala/src/main/scala/charging/ChargingStation.scala) { #state }

Java
:  @@snip [ChargingStation.java](/samples/grpc/local-drone-control-java/src/main/java/charging/ChargingStation.java) { #state }

### Command handler

The command handler is in fact two separate handlers, one for when the entity is not yet initialized, only accepting
the `Create` command, and one that is used to handle commands once the station has been initialized.

The `StartCharging` command is the only one that requires more complicated logic: if a slot is free, persist a `ChargingStarted`
event and tell the drone when charging will be done. If all charging slots are busy the reply will instead be when the first slot will
be free again and the drone can come back and try charge again.

Scala
:  @@snip [ChargingStation.scala](/samples/grpc/local-drone-control-scala/src/main/scala/charging/ChargingStation.scala) { #commandHandler }

Java
:  @@snip [ChargingStation.java](/samples/grpc/local-drone-control-java/src/main/java/charging/ChargingStation.java) { #commandHandler }

### Event handler

Just like the command handler the event handler is different depending on if there is state or not. If there is no state
only the `Created` event is accepted.

Once initialized the charging station expects `ChargingStarted` and `ChargingCompleted` events, additional `Created` 
events are ignored.

Scala
:  @@snip [ChargingStation.scala](/samples/grpc/local-drone-control-scala/src/main/scala/charging/ChargingStation.scala) { #eventHandler }

Java
:  @@snip [ChargingStation.java](/samples/grpc/local-drone-control-java/src/main/java/charging/ChargingStation.java) { #eventHandler }

@@@ note

The charging station is a very limited replicated entity example to keep the guide simple. It doesn't expect any possible 
conflicts, stations are created, once, in the central cloud and replicated to the edge, updates related to drones currently 
charging in the station happen at the edge and are replicated to the cloud. Akka replicated event sourcing provides APIs 
for both CRDTs where conflicts are automatically handled by the data structure and business domain level conflict resolution. 
For more details about see the @extref[Akka documentation](akka:typed/replicated-eventsourcing.html).

@@@

### Tagging based on location

To be able to control where the charging station is replicated we tag the events using the location id from the state as 
a topic:

Scala
:  @@snip [ChargingStation.scala](/samples/grpc/local-drone-control-scala/src/main/scala/charging/ChargingStation.scala) { #tagging }

Java
:  @@snip [ChargingStation.java](/samples/grpc/local-drone-control-java/src/main/java/charging/ChargingStation.java) { #tagging }

### Setting up replication for the charging station

Setup for the cloud replica and the edge node differs slightly. 

For the restaurant-drone-deliveries service running in the cloud we set up a `ReplicationSettings` with edge replication
enabled:

Scala
:  @@snip [ChargingStation.scala](/samples/grpc/local-drone-control-scala/src/main/scala/charging/ChargingStation.scala) { #replicaInit }

Java
:  @@snip [ChargingStation.java](/samples/grpc/local-drone-control-java/src/main/java/charging/ChargingStation.java) { #replicaInit }

Since we have other events going over akka-projection-grpc producer push replication already in the restaurant-drone-deliveries service
we need to combine all such sources and destinations into single gRPC services:

Scala
:  @@snip [Main.scala](/samples/grpc/restaurant-drone-deliveries-service-scala/src/main/scala/central/Main.scala) { #replicationEndpoint }

Java
:  @@snip [Main.java](/samples/grpc/restaurant-drone-deliveries-service-java/src/main/java/central/Main.java) { #replicationEndpoint }

For the local-drone-control service we also create `ReplicationSettings` but pass them to a separate initialization method `Replication.grpcEdgeReplication`.
Since the edge node will be responsible for creating connections, no gRPC services needs to be bound: 

Scala
:  @@snip [ChargingStation.scala](/samples/grpc/local-drone-control-scala/src/main/scala/charging/ChargingStation.scala) { #edgeReplicaInit }

Java
:  @@snip [ChargingStation.java](/samples/grpc/local-drone-control-java/src/main/java/charging/ChargingStation.java) { #edgeReplicaInit }

The returned `EdgeReplication` instance gives us access to a `entityRefFactory` for sending messages to the charging stations.


## Service for interacting with the charging station

In the restaurant-drone-deliveries service we introduce a separate gRPC endpoint for creating and looking at charging 
station state:

Scala
:  @@snip [charging_station_api.proto](/samples/grpc/restaurant-drone-deliveries-service-java/src/main/protobuf/charging/charging_station_api.proto) { }

Java
:  @@snip [charging_station_api.proto](/samples/grpc/restaurant-drone-deliveries-service-scala/src/main/protobuf/charging/charging_station_api.proto) { }

The service implementation takes the `entityRefFactory` as a constructor parameter and uses that to create `EntityRef` instances
for specific charging stations to interact with them:

Scala
:  @@snip [ChargingStationServiceImpl.scala](/samples/grpc/restaurant-drone-deliveries-service-scala/src/main/scala/charging/ChargingStationServiceImpl.scala) { }

Java
:  @@snip [ChargingStationServiceImpl.java](/samples/grpc/restaurant-drone-deliveries-service-java/src/main/java/charging/ChargingStationServiceImpl.java) { }

## Interacting with the charging station at the edge

The local-drone-control service does not contain the full gRPC API for creating and inspecting charging stations but 
instead two methods in the drone gRPC service `goCharge` to initiate charging of a drone if possible and `completeCharge`
to complete a charging session for a given drone:

Scala
:  @@snip [DroneServiceImpl.scala](/samples/grpc/local-drone-control-scala/src/main/scala/local/drones/DroneServiceImpl.scala) { #charging }

Java
:  @@snip [DroneServiceImpl.java](/samples/grpc/local-drone-control-java/src/main/java/local/drones/DroneServiceImpl.java) { #charging }


## Running

The complete sample can be downloaded from GitHub, but note that it also includes the next steps of the guide:

* Scala [drone-scala.zip](../attachments/drone-scala.zip)
* Java [drone-java.zip](../attachments/drone-java.zip)

As this service consumes events from the service built in the previous step, start the local-drone-control service first:

@@@ div { .group-scala }

To start the local-drone-control-service:

```shell
sbt run
```

@@@

@@@ div { .group-java }

```shell
mvn compile exec:exec
```

@@@

Then start the drone-restaurant-deliveries-service.

As the service needs a PostgreSQL instance running, start that up in a docker container and create the database
schema if you did not do that in a previous step of the guide:

```shell
docker compose up --wait
docker exec -i postgres_db psql -U postgres -t < ddl-scripts/create_tables.sql
```

Then start the service:

@@@ div { .group-scala }

```shell
sbt -Dconfig.resource=local1.conf run
```

And optionally one or two more Akka cluster nodes, but note that the local drone controls
are statically configured to the gRPC port of the first and will only publish events to that node.

```shell
sbt -Dconfig.resource=local2.conf run
sbt -Dconfig.resource=local3.conf run
```

@@@

@@@ div { .group-java }

```shell
mvn compile exec:exec -DAPP_CONFIG=local1.conf
```

And optionally one or two more Akka cluster nodes, but note that the local drone controls
are statically configured to the gRPC port of the first and will only publish events to that node.

```shell
mvn compile exec:exec -DAPP_CONFIG=local2.conf
mvn compile exec:exec -DAPP_CONFIG=local3.conf
```

@@@

Set up a replicated charging station in the cloud service using [grpcurl](https://github.com/fullstorydev/grpcurl):

```shell
grpcurl -d '{"charging_station_id":"station1","location_id": "sweden/stockholm/kungsholmen", "charging_slots": 4}' -plaintext localhost:8101 charging.ChargingStationService.CreateChargingStation
```

Ask to charge the drone with id `drone1` at the charging station in the edge service:

```shell
grpcurl -d '{"drone_id":"drone1","charging_station_id":"station1"}' -plaintext 127.0.0.1:8080 local.drones.DroneService.GoCharge
```

Inspect the state of the charging station in the cloud service to see the charging drone replicated there:

```shell
grpcurl -d '{"charging_station_id":"station1"}' -plaintext localhost:8101 charging.ChargingStationService.GetChargingStationState
```
Inform the station that the drone completed charging:

```shell
grpcurl -d '{"drone_id":"drone1","charging_station_id":"station1"}' -plaintext 127.0.0.1:8080 local.drones.DroneService.CompleteCharge
```

Again query the restaurant-drone-deliveries charge station inspection command to see the set of charging drones changing again:

```shell
grpcurl -d '{"charging_station_id":"station1"}' -plaintext localhost:8101 charging.ChargingStationService.GetChargingStationState
```
