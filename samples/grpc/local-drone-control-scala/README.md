# Local Drone Control Service

The sample show-cases an edge control service for drones doing restaurant deliveries,
located geographically close to the actual drones for short latencies and resilience. 

Drones interact with the closest control service in the following ways:

 * Report their precise location, at a high frequency
 * Ask for the next delivery to perform
 * Mark a delivery as completed

The control service interacts with the global cloud service, represented by the separate 
restaurant-drone-deliveries-service sample, in the following ways:

 * Replicates a coarse grained location of each drone to the cloud, at a lower frequency, 
   only when they change location at a coarse grained grid
 * get restaurant to home delivery orders in the geographical area of the local drone control 

## Running the sample

Start one instance with:

```shell
sbt run
```

### Drones and deliveries

Posting updated location for a drone:

```shell
grpcurl -d '{"drone_id":"drone1", "coordinates": {"longitude": 18.07125, "latitude": 59.31834}, "altitude": 5}' -plaintext 127.0.0.1:8080 local.drones.DroneService.ReportLocation
```

Request assignment of a delivery (it needs to have reported location at least once first)

```shell
grpcurl -d '{"drone_id":"drone1"}' -plaintext 127.0.0.1:8080 local.drones.DroneService.RequestNextDelivery
```

Mark the delivery as completed
```shell
grpcurl -d '{"delivery_id":"order1"}' -plaintext 127.0.0.1:8080 local.drones.DroneService.CompleteDelivery
```

Inspect the current state of the local delivery queue

```shell
grpcurl -plaintext 127.0.0.1:8080 local.drones.DeliveriesQueueService.GetCurrentQueue
```

### Charging station

Set up a replicated charging station in the restaurant-drone-deliveries:

```shell
grpcurl -d '{"charging_station_id":"station1","location_id": "sweden/stockholm/kungsholmen", "charging_slots": 4}' -plaintext localhost:8101 charging.ChargingStationService.CreateChargingStation
```

In the local-drone-control service, ask to charge the drone: 

```shell
grpcurl -d '{"drone_id":"drone1","charging_station_id":"station1"}' -plaintext 127.0.0.1:8080 local.drones.DroneService.GoCharge
```

Use the restaurant-drone-deliveries charge station inspection command to see the charging drones after a short delay:

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


## Running the sample as a multi node service

It is also possible to run this sample service as a multi node Akka Cluster, for that you need to start a PostgreSQL
instance for all nodes to use for storage and create schema for it:

```shell
docker compose up --wait
docker exec -i local_drone_control_db psql -U postgres -t < ddl-scripts/create_tables.sql
```

Start 3 nodes, in separate terminals:

```shell
sbt -Dconfig.resource=local1.conf "runMain local.drones.ClusteredMain"
sbt -Dconfig.resource=local2.conf "runMain local.drones.ClusteredMain"
sbt -Dconfig.resource=local3.conf "runMain local.drones.ClusteredMain"
```

The nodes now accept plaintext gRPC requests on ports 8080, 8081, 8082 