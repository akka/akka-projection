# Drone Charging Station

To showcase active-active replication between edge and cloud we'll now add a @extref[Replicated Event Sourced Entity](akka-projection:grpc-replicated-event-sourcing-transport.html)
in the form of a charging station. The charging stations are created with a location id placing them in one of the 
local-drone-control edge services where the entity is replicated. Drones in that location can request to charge in the
charging station, and be charged if there is a free charging slot.

## Charging station entity

The API for creating a replicated event sourced behavior extends the regular event sourced behavior. It accepts three
different commands from the outside `Create` to initialize a charging station, `StartCharging` to start a charging session
for a drone if possible and `GetState` to query the station for its current state. There is also a private `ChargingCompleted`
command that the entity can send to itself.

The `Create` command leads to a `Created` event which is persisted and initialized the charging station.

When a slot is free and a drone requests charging a `ChargingStarted` event is persisted and once charging a drone has
completed a `ChargingCompleted` event is persisted:

Scala
:  @@snip [ChargingStation.scala](/samples/grpc/local-drone-control-scala/src/main/scala/charging/ChargingStation.scala) { #commands #events }

Java
:  @@snip [ChargingStation.java](/samples/grpc/local-drone-control-java/src/main/java/charging/ChargingStation.java) { #commands #events }



The charging station is a simplified replicated entity in that it doesn't really expect any possible conflicts, stations
are created in the central cloud and replicated to the edge, updates related to drones currently charging in the station
happen at the edge and are replicated to the cloud. Akka replicated event sourcing provides APIs for both CRDTs where
conflicts are automatically handled by the data structure and business domain level conflict resolution. For more details 
about see the @extref[Akka documentation](akka:replicated-eventsourcing.html).