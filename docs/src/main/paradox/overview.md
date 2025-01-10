# Overview

The purpose of Akka Projections is described in @ref:[Use Cases](use-cases.md).

In Akka Projections you process a stream of events or records from a source to a projected model or external system.
Each event is associated with an offset representing the position in the stream. This offset is used for
resuming the stream from that position when the projection is restarted.

As the source you can select from:

* @ref:[Events from Akka Persistence](eventsourced.md)
* @ref:[State changes from Akka Persistence](durable-state.md)
* @ref:[Events from Akka Persistence published over gRPC](grpc.md)
* @ref:[Messages from Kafka](kafka.md)
* Building your own @apidoc[SourceProvider]

For the offset storage you can select from:

* @ref:[Offset in Cassandra](cassandra.md)
* @ref:[Offset in a relational DB with JDBC](jdbc.md)
* @ref:[Offset in a relational DB with R2DBC](r2dbc.md)
* @ref:[Offset in a relational DB with Slick](slick.md) (community-driven module)

Those building blocks are assembled into a `Projection`. You can have many instances of it
@ref:[automatically distributed and run](running.md) in an Akka Cluster.

To see a complete example of an Akka Projections implementation review the @ref:[Getting Started Guide](getting-started/index.md)
or the @extref[Microservices with Akka tutorial](platform-guide:microservices-tutorial/).

## Dependencies

Akka Projections consists of several modules for specific technologies. The dependency section for
each module describes which dependency you should define in your project.

* @ref:[Events from Akka Persistence](eventsourced.md)
* @ref:[State changes from Akka Persistence](durable-state.md)
* @ref:[Messages from Kafka](kafka.md)
* @ref:[Offset in Cassandra](cassandra.md)
* @ref:[Offset in a relational DB with JDBC](jdbc.md)
* @ref:[Offset in a relational DB with Slick](slick.md) (community-driven module)

All of them share a dependency to `akka-projection-core`: 

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-core_$scala.binary.version$
  version=$project.version$
}

@@project-info{ projectId="akka-projection-core" }

### Akka version

Akka Projections requires **Akka $akka.version$** or later. See [Akka's Binary Compatibility Rules](https://doc.akka.io/libraries/akka-core/current/common/binary-compatibility-rules.html) for details.

It is recommended to use the latest patch version of Akka. 
It is important all Akka dependencies are in the same version, so it is recommended to depend on
them explicitly to avoid problems with transient dependencies causing an unlucky mix of versions. For example:

@@dependency[sbt,Gradle,Maven] {
  symbol=AkkaVersion
  value=$akka.version$
  group=com.typesafe.akka
  artifact=akka-cluster-sharding-typed_$scala.binary.version$
  version=AkkaVersion
  group2=com.typesafe.akka
  artifact2=akka-persistence-query_$scala.binary.version$
  version2=AkkaVersion
  group3=com.typesafe.akka
  artifact3=akka-discovery_$scala.binary.version$
  version3=AkkaVersion
}

### Transitive dependencies

The table below shows `akka-projection-core`'s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-core" }

See the individual modules for their transitive dependencies.

### Akka Classic

Akka Projections can be used with the [new Actor API](https://doc.akka.io/libraries/akka-core/current/typed/actors.html) or
the [classic Actor API](https://doc.akka.io/libraries/akka-core/current/index-classic.html). The documentation samples
show the new Actor API, and the @ref:[Akka Classic page](classic.md) highlights how to use it with the classic
Actor API.

## Contributing

Please feel free to contribute to Akka and Akka Projections by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/akka/blob/main/CONTRIBUTING.md) to learn how it can be done.

We want Akka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://akka.io/conduct).
