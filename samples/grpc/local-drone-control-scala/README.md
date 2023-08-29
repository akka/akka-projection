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