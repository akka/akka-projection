# Example Use Cases

## Car telemetry aggregation

A car company collects and aggregates telemetry data from millions of cars around the world. A car reports its status
every third second to a backend service. The cars are connected to a nearby cloud region for low-latency and high
availability reasons. All telemetry data is fed into an analytics system to find anomalies and gather statistics,
but it would be an extreme amount of data and an unnecessary cost to transfer all telemetry samples to the global analytics
system. Instead, the data is aggregated at the "local" regions and then transferred less frequent, but still
near real time, to the analytics system.

When an anomaly is found by the analytics system it can dynamically change the level of detail for certain categories
of cars during some time period.

## Redundant shopping cart

An ecommerce business makes it possible for a group of people, e.g. a family or a team at a company, to share a
shopping cart. Items are collected in the cart from individual sessions and finally checked out to order the goods.
The shopping cart is critical for the business and must always be available. For redundancy, the service is
deployed to several cloud regions and two different cloud providers. The shopping cart can be modified by several
people at the same time and also from several locations, such as cloud regions and cloud providers, at the same time.
In case of region outage or network outage between the regions or cloud providers the cart can still be modified by
the users connected to an available service. Simultaneous updates at different locations will be consolidated when
the problem has been solved.

Additionally, for best interactivity with low response times the users are connected to a nearby cloud region.
It would be unnecessary to replicate all shopping carts to all cloud regions and providers and therefore the
carts are replicated on demand to a region when a user of the cart connects to a new region.

## World-wide online auction

An online auction system is running in 20 regions around the globe. There are many active auctions in total,
but all are not active in all regions since users connect to the closest region. Some information about the
auctions are replicated to all regions, such as the list and summary status of active auctions. Replicating
all bids and comments for all auctions to all regions would be wasteful. When a user shows interest in a
specific auction, such as placing a bid or asking a question, the full history for that auction is replicated
to the region of the user and updates are immediately replicated to the regions that participate in the auction.
After inactivity in a region for a specific auction the replication is stopped.

## Low cost Microservices

A startup company would like to keep infrastructure costs low but still use a Reactive Systems architecture
that can grow with their upcoming successful business. They use Akka to implement the Microservices. For
the communication between the services they don't want the pay the cost of using a hosted message broker or
the burden of operating a message broker infrastructure themselves. They use Akka Distributed Cluster for the
service-to-service communication.
