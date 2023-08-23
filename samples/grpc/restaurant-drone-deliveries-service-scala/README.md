# Restaurant Drone Deliveries Service

The sample show-cases a service for drones doing restaurant deliveries.

It is intended to be used together with the local-drone-control sample.

* Keeps track of a coarse grained location of each drone to the cloud
* FIXME Accepts restaurant delivery requests which are then fed to the right local drone control

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
sbt -Dconfig.resource=local1.conf run
```

3. (Optional) Start another node with different ports:

```shell
sbt -Dconfig.resource=local2.conf run
```

4. (Optional) More can be started:

 ```shell
sbt -Dconfig.resource=local3.conf run
 ```

5. Check for service readiness

 ```shell
curl http://localhost:9101/ready
 ```
   

Query a location for drone coordinates

```shell
grpcurl -d '{"location":"kungsholmen"}' -plaintext localhost:8101 central.drones.DroneOverviewService/GetCoarseDroneLocations
```

Query the state for a specific drone

```shell
grpcurl -d '{"drone_id":"drone1"}' -plaintext localhost:8101 central.drones.DroneOverviewService.GetDroneOverview
```

Set up a restaurant

```shell
grpcurl -d '{"restaurant_id":"restaurant1","coordinates":{"longitude": 59.33252, "latitude": 18.04142}}' -plaintext localhost:8101 central.deliveries.RestaurantDeliveriesService.SetUpRestaurant
```

Register a delivery for the restaurant

```shell
grpcurl -d '{"restaurant_id":"restaurant1","delivery_id": "order1","coordinates":{"longitude": 59.13212, "latitude": 18.12442}}' -plaintext localhost:8101 central.deliveries.RestaurantDeliveriesService.RegisterDelivery
```

FIXME more stuff