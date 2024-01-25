# Guide - Java/Scala

As a hands-on guide we will step by step implement a drone restaurant to customer delivery service where orders for deliveries
between restaurants and customers are created in a cloud hosted service.

The drones continuously inform their PoP (point of presence) local control center of their exact location. An approximate
location of the drones is replicated to the cloud service at a much lower frequency, only when the drones change 
location on a coarse grained GPS coordinate grid.

The cloud service accepts restaurant orders and replicates them to the right control center. Drones interact with
the local center to pick up available orders closest to their location.

@@toc { depth=2 }

@@@ index

1. [Local Drone Control Service](guide/1-local-drone-control-service.md)
2. [Coarse Grained Location Replication](guide/2-drone-location-to-cloud-service.md)
3. [Restaurant Deliveries Service](guide/3-restaurant-deliveries-service.md)
4. [Local Drone Delivery Selection](guide/4-local-drone-delivery-selection.md)
5. [Drone Charging Station](guide/5-charging-station.md)
6. [Deploying the Services](guide/6-deploying-the-services.md)

@@@

