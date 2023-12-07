# Restaurant Drone Deliveries Service

The sample show-cases a service for drones doing restaurant deliveries.

It is intended to be used together with the local-drone-control sample.

* Keeps track of a coarse grained location of each drone to the cloud
Accepts restaurant delivery requests which are then replicated to the right local drone control

## Running the sample code

1. Start a local PostgresSQL server on default port 5432. The included `docker-compose.yml` starts everything required for running locally.

 ```shell
 docker compose up --wait

 # creates the tables needed for Akka Persistence
 # as well as the offset store table for Akka Projection
 docker exec -i postgres_db psql -U postgres -t < ddl-scripts/create_tables.sql
 ```

2. Start a first node:

```shell
mvn compile exec:exec -DAPP_CONFIG=local1.conf
```

3. (Optional) Start another node with different ports:

```shell
mvn compile exec:exec -DAPP_CONFIG=local2.conf
```

4. (Optional) More can be started:

```shell
mvn compile exec:exec -DAPP_CONFIG=local3.conf
```

5. Check for service readiness

 ```shell
curl http://localhost:9101/ready
 ```
   

Query a location for drone coordinates

```shell
grpcurl -d '{"location":"sweden/stockholm/kungsholmen"}' -plaintext localhost:8101 central.drones.DroneOverviewService/GetCoarseDroneLocations
```

Query the state for a specific drone

```shell
grpcurl -d '{"drone_id":"drone1"}' -plaintext localhost:8101 central.drones.DroneOverviewService.GetDroneOverview
```

Set up a restaurant

```shell
grpcurl -d '{"restaurant_id":"restaurant1","coordinates":{"latitude": 59.330324, "longitude": 18.039568}, "local_control_location_id": "sweden/stockholm/kungsholmen" }' -plaintext localhost:8101 central.deliveries.RestaurantDeliveriesService.SetUpRestaurant
```

Set up another restaurant, closest to a different local drone control

```shell
grpcurl -d '{"restaurant_id":"restaurant2","coordinates":{"latitude": 59.342046, "longitude": 18.059095}, "local_control_location_id": "sweden/stockholm/norrmalm" }' -plaintext localhost:8101 central.deliveries.RestaurantDeliveriesService.SetUpRestaurant
```

Register a delivery for the first restaurant

```shell
grpcurl -d '{"restaurant_id":"restaurant1","delivery_id": "order1","coordinates":{"latitude": 59.330841, "longitude": 18.038885}}' -plaintext localhost:8101 central.deliveries.RestaurantDeliveriesService.RegisterDelivery
```

Register a delivery for the second restaurant

```shell
grpcurl -d '{"restaurant_id":"restaurant2","delivery_id": "order2","coordinates":{"latitude": 59.340128, "longitude": 18.056303}}' -plaintext localhost:8101 central.deliveries.RestaurantDeliveriesService.RegisterDelivery
```

Set up a replicated charging station

```shell
grpcurl -d '{"charging_station_id":"station1","location_id": "sweden/stockholm/kungsholmen", "charging_slots": 4}' -plaintext localhost:8101 charging.ChargingStationService.CreateChargingStation
```

Inspect the state of the charging station

```shell
grpcurl -d '{"charging_station_id":"station1"}' -plaintext localhost:8101 charging.ChargingStationService.GetChargingStationState