# Architectural Overview

Akka Cluster is great for building stateful distributed systems, such as Microservices. Akka Distributed Cluster 
is a set of features in Akka that will help you with:

* stretching an Akka Cluster over geographically distributed locations for higher availability and lower latency
* asynchronous communication between different Akka Microservices 

## One Akka Cluster or many connected clusters?

When stretching an Akka Cluster over geographically distributed locations it can be difficult or impossible to
make it work as one single Akka Cluster. Those locations are typically also different Kubernetes clusters. There can be
many problems that must be overcome:

* security considerations of opening up for the peer-to-peer communication between nodes that is required for Akka Cluster
* address translation and connectivity of separate Kubernetes clusters
* reliable Akka Cluster bootstrap mechanism across different Kubernetes clusters
* failure detection and stability of an Akka Cluster with mixed low and high latency

Instead, each geographical location can be a separate, fully autonomous, Akka Cluster and be connected to the other
Akka Clusters with the communication mechanisms provided by Akka Distributed Cluster.

The communication transport between the Akka Clusters is then using reliable event replication over gRPC, which
gives characteristics such as:

* security with TLS and mutual authentication (mTLS)
* network friendly across different Kubernetes clusters using ordinary ingress and load balancers
* exactly-once processing or at-least-once processing
* asynchronous and brokerless communication without need of additional products

For Microservices there are additional reasons for keeping the services isolated in separate Akka Clusters as
described in @extref[Choosing Akka Cluster - Microservices](akka:typed/choosing-cluster.html).
