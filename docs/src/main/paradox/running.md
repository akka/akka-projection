# Running a Projection

Once you have decided how you want to build your projection, the next step is to run it. Typically, you run it in a distributed fashion in order to spread the load over the different nodes in an Akka Cluster. However, it's also possible to run it as a single instance (when not clustered) or as single instance in a Cluster Singleton.

## Dependencies

To distribute the projection over the cluster we recommend the use of [SharderDaemonProcess](https://doc.akka.io/docs/akka/current/typed/cluster-sharded-daemon-process.html). Add the following dependency in your project if not yet using Akka Cluster Sharding:

@@dependency [sbt,Maven,Gradle] {
  group=com.typesafe.akka
  artifact=akka-cluster-sharding-typed_$scala.binary.version$
  version=2.6.5
}

Akka Projections require Akka 2.6.5 or later, see Akka version.

For more information on using Akka Cluster consult Akka's reference on [Akka Cluster](https://doc.akka.io/docs/akka/current/typed/index-cluster.html) and [Akka Cluster Sharding](https://doc.akka.io/docs/akka/current/typed/cluster-sharding.html).

## Sharded Daemon Process Example

The Sharded Daemon Process can be used to distribute `n` instances of a given Projection across the cluster. Therefore, it's important that each Projection instance consumes a subset of the stream of envelopes.

How the subset is created depends on the kind of source we consume. If it's a Alpakka Kafka source, this is typically done using Kafka partitions. When consuming from Akka Persistence Journal, the events must be tagged with a shard number as demonstrate in the example bellow.

### Tagging Events in Akka Persistence

Before we can consume the events, the entity must tag the events with a shard number.

Scala
:  @@snip [ShoppingCart.scala](/examples/src/test/scala/docs/eventsourced/ShoppingCart.scala) { #imports #shardingTags }

In the above example, we created an `IndexedSeq` of tags from *carts-0* to *carts-4*. Each entity instance will tag its events using one of those tags. The tag is selected based on the module of the entity id's hash code (stable identifier) and the total number of tags. As a matter of fact, this will create a journal sliced with different tags (ie: from *carts-0* to *carts-4*).

We will use those tags to query the journal and create as much as Projections instances, and distribute them on the cluster.

### Event Sourced Provider per tag

We can use @ref:[EventSourcedProvider.eventsByTag](eventsourced.md) to consume the `ShoppingCart` events.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #source-provider-imports #running-source-provider }

Note that we define a method that builds a new `SourceProvider` for each passed `tag`.

### Building the Projection instances

Next we create a method to return Projection instances. Again, we pass a tag that is used to initialize the `SourceProvider` and as the key in `ProjectionId`.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #projection-imports #running-projection }

### Initializing the Sharded Daemon

Once we have the tags, the `SourceProvider` and the `Projection` of our choice, we can glue all the pieces together using the Sharded Daemon Process and let it be distributed across the cluster.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #daemon-imports #running-with-daemon-process }

For this example, we configure as much `ShardedDaemonProcess` as tags and we define the behavior factory to return `ProjectionBehavior` wrapping each time a different `Projection` instance. Finally, the Sharded Daemon to use the `ProjectionBehavior.Stop` as its control stop message.

### Projection Behavior

The `ProjectionBehavior` is an Actor Behavior that knows how to manage the Projection lifecyle. The Projection starts to consume the events as soon as the actor is spawn and will restart the source in case of failures (see @ref:[Projection Settings](projection-settings.md)).
