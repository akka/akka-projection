# Akka Edge

Akka Edge is a set of features in Akka that will help you with using Akka at the edge of the cloud for higher
availability and lower latency.

![Diagram showing an overview of Akka Edge](images/overview.svg)

Akka Edge is similar to @extref[Akka Distributed Cluster](akka-distributed-cluster:) and they share features and
implementation, but Akka Edge is intended for use cases with even more geographically distributed services and
possibly in more resource constrained environments. In contrast, Akka Distributed Cluster is intended for
connecting rather few services in different cloud regions. Akka Edge and Akka Distributed Cluster are designed
to be used together.

.

@@toc { depth=2 }

@@@ index

* [Overview](overview.md)
* [Example Use Cases](use-cases.md)
* [Feature Summary](feature-summary.md)
* [Guide](guide.md)
* [Lightweight deployments](lightweight-deployments.md)

@@@
