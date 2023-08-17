# Local Drone Control Service

The sample show-cases an edge control service for drones doing restaurant deliveries,
located geographically close to the actual drones for short latencies and resilience. 

Drones interact with the closest control service in the following ways:

 * Report their precise location, at a high frequency
 * FIXME Ask for the next delivery to perform

The control service interacts with the global cloud service, represented by the separate 
restaurant-drone-deliveries-service sample, in the following ways:

 * Replicates a coarse grained location of each drone to the cloud, at a lower frequency, 
   only when they change location at a coarse grained grid
 * FIXME get restaurant to home delivery orders in the geographical area of the local drone control 

## Running the sample

Start one instance with:

```
sbt run
```

FIXME more stuff