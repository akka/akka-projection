# Akka Replicated Event Sourcing over gRPC

Akka Replicated Event Sourcing extends Akka Persistence allowing multiple replicas of the same entity, all accepting
writes, for example in different data centers or cloud provider regions. This makes it possible to implement patterns 
such as active-active and hot standby. 

Originally, Akka Replicated Event Sourcing has required cross-replica access to the underlying replica database, which
can be hard to open up for security and infrastructure reasons.

Akka Replicated Event Sourcing over gRPC builds on @ref:[Akka Projection gRPC](grpc.md) and @extref:[Akka gRPC](akka-grpc:index.html) to instead use gRPC as the cross-replica transport for events.

@@@ warning

This module is currently marked as [May Change](https://doc.akka.io/docs/akka/current/common/may-change.html)
in the sense that the API might be changed based on feedback from initial usage.
However, the module is ready for usage in production and we will not break serialization format of
messages or stored data.

@@@

## Overview

For a basic overview of Replicated Event Sourcing see the @extref:[Akka Replicated Event Sourcing docs](akka:typed/replicated-eventsourcing.html)

Akka Replicated Event Sourcing over gRPC consists of the following three parts:

The Replicated Event Sourced Behavior is run in each replica as a sharded entity using @extref:[Akka Cluster Sharding](akka:typed/cluster-sharding.html).

The events of each replica are published to the other replicas using @ref:[Akka Projection gRPC](grpc.md) endpoints.

Each replica consumes a number of parallel slices of the events from each other replica by running Akka Projection gRPC
in @extref:[Akka Sharded Daemon Process](akka:typed/cluster-sharded-daemon-process.html).



## Dependencies

The functionality is provided through the `akka-projection-grpc` module.

@@project-info{ projectId="akka-projection-grpc" }

To use the gRPC module of Akka Projections add the following dependency in your project:

Akka Replicated Event Sourcing over gRPC requires Akka 2.8.0 or later and can only be run in an Akka cluster since it uses cluster components.

It is currently only possible to use @extref:[akka-persistence-r2dbc](akka-persistence-r2dbc:projection.html) as the
projection storage and journal for this module.

The full set of dependencies needed:

@@dependency [sbt,Maven,Gradle] {
group=com.lightbend.akka
artifact=akka-projection-grpc_$scala.binary.version$
version=$project.version$
group2=com.typesafe.akka
artifact2=akka-cluster-typed_$scala.binary.version$
version2=$akka.version$
group3=com.typesafe.akka
artifact3=akka-cluster-sharding-typed_$scala.binary.version$
version3=$akka.version$
group4=com.lightbend.akka
artifact4=akka-persistence-r2dbc_$scala.binary.version$
version4=$akka.r2dbc.version$
group5=com.lightbend.akka
artifact5=akka-projection-r2dbc_$scala.binary.version$
version5=$akka.r2dbc.version$
}

### Transitive dependencies

The table below shows `akka-projection-grpc`'s direct dependencies, and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-grpc" }



### Settings

The @apidoc[akka.projection.grpc.replication.*.ReplicationSettings] @scala[`apply`]@java[`create`] factory methods can 
accept an entity name, a @apidoc[ReplicationProjectionProvider] and an actor system. The configuration of that system
is expected to have a top level entry with the entity name containing this structure:

Scala
:  @@snip [config](/akka-projection-grpc/src/test/scala/akka/projection/grpc/replication/ReplicationSettingsSpec.scala) { #config }

Java
:  @@snip [config](/akka-projection-grpc/src/test/scala/akka/projection/grpc/replication/ReplicationSettingsSpec.scala) { #config }

The entries in the block refer to the local replica while `replicas` is a list of all replicas, including the node itself, 
with details about how to reach the replicas across the network. 

The `grpc.client` section for each of the replicas is used for setting up the Akka gRPC client and supports the same discovery, TLS
and other connection options as when using Akka gRPC directly. For more details see @extref:[Akka gRPC configuration](akka-grpc:client/configuration.html#by-configuration).

It is also possible to set up @apidoc[akka.projection.grpc.replication.*.ReplicationSettings] through APIs only and not rely
on the configuration file at all.

### Binding the publisher

Binding the publisher is a manual step to allow arbitrary customization of the Akka HTTP server and combining the endpoint
with other HTTP and gRPC routes.

When there is only a single replicated entity and no other usage of Akka gRPC Projections in an application a
convenience is provided through `createSingleServiceHandler` on @apidoc[akka.projection.grpc.replication.*.Replication] which
will create a single handler, this can then be bound:

FIXME sample snippet

When multiple producers exist, all instances of @apidoc[akka.projection.grpc.producer.EventProducerSettings] need to
be passed at once to `EventProducer.grpcServiceHandler` to create a single producer service handling each of the event
streams.

FIXME sample snippet

The Akka HTTP server must be running with HTTP/2, this is done through config:

Scala
:  @@snip [ShoppingCartServer.scala](/samples/grpc/shopping-cart-service-scala/src/main/resources/grpc.conf) { #http2 }

Java
:  @@snip [ShoppingCartServer.java](/samples/grpc/shopping-cart-service-java/src/main/resources/grpc.conf) { #http2 }

### Serialization of events

The events are serialized for being passed over the wire using the same Akka serializer as configured for serializing
the events for storage. 

Note that having separate replicas increases the risk that two different serialized formats and versions of the serializer
are running at the same time, so extra care must be taken when changing the events and their serialization and deploying
new versions of the application to the replicas.

FIXME something more here - serialization can fail and stop the replication, but it could also silently lose data in new fields
before the consuming side has a new version.

### Sample projects

FIXME should we have one? The shopping cart?

## Access control

### From the consumer

The consumer can pass metadata, such as auth headers, in each request to the producer service by specifying @apidoc[akka.grpc.*.Metadata] as `additionalRequestMetadata` when creating each @apidoc[akka.projection.grpc.replication.*.Replica]

### In the producer

Authentication and authorization for the producer can be done by implementing an @apidoc[EventProducerInterceptor] and passing
it to the `grpcServiceHandler` method during producer bootstrap. The interceptor is invoked with the stream id and
gRPC request metadata for each incoming request and can return a suitable error through @apidoc[GrpcServiceException]