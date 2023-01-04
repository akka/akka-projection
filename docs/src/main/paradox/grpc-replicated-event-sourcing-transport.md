# Akka Replicated Event Sourcing over gRPC

Akka Replicated Event Sourcing extends Akka Persistence allowing multiple replicas of the same entity, all accepting
writes, for example in different data centers or cloud provider regions. This makes it possible to implement patterns 
such as active-active and hot standby. 

Originally Akka Replicated Event Sourcing has required cross replica access to the underlying replica database which
can be hard to open up for security and infra structure reasons.

Akka Replicated Event Sourcing over gRPC builds on @ref:[Akka Projection gRPC](grpc.md) and @extref:[Akka gRPC](akka-grpc:index.html)  to instead use gRPC as the cross replica transport for events.

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

The events of the replica is published to the other replicas using @ref:[Akka Projection gRPC](grpc.md) endpoints.

Each replica consumes a number of parallel slices of the events from each other replica by running Akka Projection gRPC
in @extref:[Akka Sharded Daemon Process](akka:typed/cluster-sharded-daemon-process.html).



## Dependencies

To use the R2DBC module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
group=com.lightbend.akka
artifact=akka-projection-grpc_$scala.binary.version$
version=$project.version$
}

Akka Replicated Event Sourcing over gRPC require Akka 2.8.0 or later and can only be run in an Akka cluster since it uses cluster components.

The functionality is provided through the `akka-projection-grpc` module. 

@@project-info{ projectId="akka-projection-grpc" }

### Transitive dependencies

The table below shows `akka-projection-grpc`'s direct dependencies, and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-grpc" }


### Serialization of events

FIXME is there anything here?

### Binding the publisher

Binding the publisher is a manual step to allow arbitrary customization of the Akka HTTP server and combining the endpoint
with other HTTP and gRPC routes.

When there is only a single replicated entity and no other usage of Akka gRPC Projections in an application a 
convenience is provided through `createSingleServiceHandler` on @apidoc[akka.projection.grpc.replication.*.Replication] which
will create a single handler, this can then be bound:

FIXME sample snippet

When multiple producers exist, all instances of @apidoc[akka.projection.grpc.producer.EventProducerSettings] needs to
be passed at once to `EventProducer.grpcServiceHandler` to create a single producer service handling each of the event
streams.

FIXME sample snippet


### Sample projects

FIXME should we have one? The shopping cart?

## Access control

### From the consumer

The consumer can pass metadata, such as auth headers, in each request to the producer service by specifying @apidoc[akka.grpc.*.Metadata] as `additionalRequestMetadata` when creating each @apidoc[akka.projection.grpc.replication.Replica]

### In the producer

Authentication and authorization for the producer can be done by implementing a @apidoc[EventProducerInterceptor] and pass
it to the `grpcServiceHandler` method during producer bootstrap. The interceptor is invoked with the stream id and
gRPC request metadata for each incoming request and can return a suitable error through @apidoc[GrpcServiceException]

### TLS

FIXME we should probably recommend TLS (and maybe mTLS) and at least link to how to set that up or maybe show? We have a short section in the akka-guide.