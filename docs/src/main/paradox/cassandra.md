# Offset in Cassandra

The @apidoc[CassandraProjection$] has support for storing the offset in Cassandra.

The source of the envelopes can be @ref:[events from Akka Persistence](eventsourced.md) or any other `SourceProvider`
with supported @ref:[offset types](#offset-types).

The envelope handler can integrate with anything, such as publishing to a message broker, or updating a read model
in Cassandra.

The `CassandraProjection` offers @ref:[at-least-once](#at-least-once) and @ref:[at-most-once](#at-most-once)
processing semantics, but not exactly-once.

## Dependencies

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

To use the Cassandra module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-cassandra_$scala.binary.version$
  version=$project.version$
}

Akka Projections requires Akka $akka.version$ or later, see @ref:[Akka version](overview.md#akka-version).

@@project-info{ projectId="akka-projection-cassandra" }

### Transitive dependencies

The table below shows `akka-projection-cassandra`'s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-cassandra" }

## at-least-once

The offset is stored after the envelope has been processed and giving at-least-once processing semantics.
This means that if the projection is restarted from a previously stored offset some elements may be processed more
than once. Therefore, the @ref:[Handler](#handler) code must be idempotent.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #projection-imports #atLeastOnce }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #projection-imports #atLeastOnce }

The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
This window can be defined with `withSaveOffset` of the returned `AtLeastOnceProjection`.
The default settings for the window is defined in configuration section `akka.projection.at-least-once`.
There is a performance benefit of not storing the offset too often, but the drawback is that there can be more
duplicates when the projection that will be processed again when the projection is restarted.

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## at-most-once

The offset for each envelope is stored before the envelope has been processed and giving at-most-once
processing semantics. This means that if the projection is restarted from previously stored offset one envelope
may not have been processed.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #projection-imports #atMostOnce }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #atMostOnce }

Since the offset must be stored for each envelope this is slower than @ref:[at-least-once](#at-least-once), which
can batch offsets before storing.

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## groupedWithin

The envelopes can be grouped before processing, which can be useful for batch updates.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #grouped }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #grouped }

The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first.
This window can be defined with `withGroup` of the returned `GroupedProjection`. The default settings for
the window is defined in configuration section `akka.projection.grouped`.

When using `groupedWithin` the handler is a @scala[`Handler[immutable.Seq[EventEnvelope[ShoppingCart.Event]]]`]@java[`Handler<List<EventEnvelope<ShoppingCart.Event>>>`].
The @ref:[`GroupedShoppingCartHandler` is shown below](#grouped-handler).

It stores the offset in Cassandra immediately after the `handler` has processed the envelopes, but that
is still with at-least-once processing semantics. This means that if the projection is restarted
from previously stored offset the previous group of envelopes may be processed more than once.

## Handler

It's in the @apidoc[Handler] that you implement the processing of each envelope. It's essentially a function
from `Envelope` to @scala[`Future[Done]`]@java[`CompletionStage<Done>`]. This means that the envelope handler
can integrate with anything, such as publishing to a message broker, or updating a read model in Cassandra.

A handler that is consuming `ShoppingCart.Event` from `eventsByTag` can look like this:

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #handler-imports #handler }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #handler-imports #handler }

@@@ note { title=Hint }
Such simple handlers can also be defined as plain @scala[functions]@java[lambdas] via the helper
@scala[`Handler.apply`]@java[`Handler.fromFunction`] factory method.
@@@

### Grouped handler

When using @ref:[`CassandraProjection.groupedWithin`](#groupedwithin) the handler is processing a @scala[`Seq`]@java[`List`] of envelopes.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #grouped-handler }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #grouped-handler }

### Stateful handler

The `Handler` can be stateful, with variables and mutable data structures. It is invoked by the `Projection` machinery
one envelope at a time and visibility guarantees between the invocations are handled automatically, i.e. no volatile
or other concurrency primitives are needed for managing the state.

The returned @scala[`Future[Done]`]@java[`CompletionStage<Done>`] is to be completed when the processing of the
`envelope` has finished. The handler will not be invoked with the next envelope until after the returned 
@scala[`Future[Done]`]@java[`CompletionStage<Done>`] has been completed.

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #mutableState }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #mutableState }

@@@ note

It is important that the `Handler` instance is not shared between several `Projection` instances,
because then it would be invoked concurrently, which is not how it is intended to be used. Each `Projection`
instance should use a new `Handler` instance. This is the reason why the handler parameter is a factory
@scala[(`() =>`)]@java[(`Supplier`)] of the handler. A new handler instance is also created when the projection
is restarted. 

@@@

However, the state must typically be loaded and updated by asynchronous operations and then it can be
error prone to manage the state in variables of the `Handler`. For that purpose a @apidoc[StatefulHandler]
is provided.

Let us look at how a `StatefulHandler` can be implemented in the context of a "word count" domain. The purpose is
to process a stream of words and for each word keep track of how many times it has occurred. 

Given an envelope and `SourceProvider` for this example:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #envelope #sourceProvider }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #envelope #sourceProvider }

and a repository for the interaction with the database:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #repository }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #repository }

The `Projection` can be definined as:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExampleSpec.scala) { #projection }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExampleTest.java) { #projection }

The `handler` can be implemented as follows.

A naive approach would be to have one row per word for storing the current count in the database. 
The handler could be implemented as a completely stateless handler that for each processed envelope loads the current
count from the database, increment the count by 1 and saved it again. Typically there will be several instances of the
`Projection` with different `ProjectionId.id`. Each `Projection` instance would be responsible for processing a subset
of all words. This stateless approach wouldn't be very efficient and you would have to use optimistic database locking to make
sure that one `Projection` instance is not overwriting the stored value from another instance without reading the right
value first.

Better would be that each `Projection` instance is a single-writer so that it can keep the current word count in
memory and only load it on startup or on demand.

A handler that is loading the state from the database when it's starting up:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #loadingInitialState }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #StatefulHandler-imports #loadingInitialState }

The `StatefulHandler` has two methods that needs to be implemented. 

* `initialState` - Invoked to load the initial state when the projection is started or if previous `process` failed.
* `process(state, envelope)` - Invoked for each `Envelope`, one at a time. The `state` parameter is the completed
  value of the previously returned @scala[`Future[State]`]@java[`CompletionStage<State>`] or the `initialState`.

If the previously returned @scala[`Future[State]`]@java[`CompletionStage<State>`] failed it will call `initialState` again and use that value.

Another implementation would be a handler that is loading the current count for a word on demand, and thereafter
caches it in the in-memory state:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #loadingOnDemand }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #StatefulHandler-imports #loadingOnDemand }

### Actor handler

A good alternative for advanced state management is to implement the handler as an [actor](https://doc.akka.io/libraries/akka-core/current/typed/actors.html),
which is described in @ref:[Processing with Actor](actor.md).

### Flow handler

An Akka Streams `FlowWithContext` can be used instead of a handler for processing the envelopes,
which is described in @ref:[Processing with Akka Streams](flow.md).

### Handler lifecycle

You can override the `start` and `stop` methods of the @apidoc[Handler] to implement initialization
before first envelope is processed and resource cleanup when the projection is stopped.
Those methods are also called when the `Projection` is restarted after failure.

See also @ref:[error handling](error.md).

## Schema

The database schema for the offset storage table.

@@@ note

The `partition` field is used to distribute projection rows across cassandra nodes while also allowing us to query all
rows for a projection name.  For most offset types we return only one row that matches the provided projection key, but
the @apidoc[MergeableOffset] requires all rows.

@@@

```
CREATE TABLE IF NOT EXISTS akka_projection.offset_store (
  projection_name text,
  partition int,
  projection_key text,
  offset text,
  manifest text,
  last_updated timestamp,
  PRIMARY KEY ((projection_name, partition), projection_key));

CREATE TABLE IF NOT EXISTS akka_projection.projection_management (
  projection_name text,
  partition int,
  projection_key text,
  paused boolean,
  last_updated timestamp,
  PRIMARY KEY ((projection_name, partition), projection_key));
```

## Offset types

The supported offset types of the `CassandraProjection` are:

* `akka.persistence.query.Offset` types from @ref:[events from Akka Persistence](eventsourced.md)
* `String`
* @scala[`Int`]@java[Integer]
* `Long`
* Any other type that has a configured Akka Serializer is stored with base64 encoding of the serialized bytes. 

@@@ note

The @apidoc[MergeableOffset] that is used for @ref:[messages from Kafka](kafka.md) is not implemented
for the `CassandraProjection` yet, see [issue #97](https://github.com/akka/akka-projection/issues/97).

@@@

The schema can be created using the method `CassandraProjection.createTablesIfNotExists`. This is particularly useful when writting tests. For production enviornments, we recommend creating the schema before deploying the application.

## Configuration

Make your edits/overrides in your application.conf.

The reference configuration file with the default values:

@@snip [reference.conf](/akka-projection-cassandra/src/main/resources/reference.conf) { #config }

### Cassandra driver configuration

All Cassandra driver settings are via its [standard profile mechanism](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/).

One important setting is to configure the database driver to retry the initial connection:

`datastax-java-driver.advanced.reconnect-on-init = true`

It is not enabled automatically as it is in the driver's reference.conf and is not overridable in a profile.

It is possible to share the same Cassandra session as [Akka Persistence Cassandra](https://doc.akka.io/libraries/akka-persistence-cassandra/current/)
by setting the `session-config-path`:

```
akka.projection.cassandra {
  session-config-path = "akka.persistence.cassandra"
}
```

or share the same Cassandra session as @extref:[Alpakka Cassandra](alpakka:cassandra.html):

```
akka.projection.cassandra {
  session-config-path = "alpakka.cassandra"
}
```

### Cassandra driver overrides

@@snip [reference.conf](/akka-projection-cassandra/src/main/resources/reference.conf) { #profile }

### Contact points configuration

The Cassandra server contact points can be defined with the [Cassandra driver configuration](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/)

```
datastax-java-driver {
  basic.contact-points = ["127.0.0.1:9042"]
  basic.load-balancing-policy.local-datacenter = "datacenter1"
}
```

Alternatively, Akka Discovery can be used for finding the Cassandra server contact points as described
in the @extref:[Alpakka Cassandra documentation](alpakka:cassandra.html#using-akka-discovery).

Without any configuration it will use `localhost:9042` as default.
