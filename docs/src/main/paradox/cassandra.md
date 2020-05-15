# Offset in Cassandra

The @apidoc[CassandraProjection] has support for storing the offset in Cassandra. 

The source of the envelopes can be @ref:[events from Akka Persistence](eventsourced.md) or any other `SourceProvider`
with supported @ref:[offset types](#offset-types).

The envelope handler can integrate with anything, such as publishing to a message broker, or updating a read model
in Cassandra.

The `CassandraProjection` offers @ref:[at-least-once](#at-least-once) and @ref:[at-most-once](#at-most-once)
processing semantics, but not exactly-once.

## Dependencies

To use the Cassandra module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.typesafe.akka
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
than once.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #atLeastOnce }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #atLeastOnce }

The `saveOffsetAfterEnvelopes` and `saveOffsetAfterDuration` parameters control how often the offset is stored.
There is a performance benefit of not storing the offset too often but the drawback is that there can be more
duplicates when the projection that will be processed again when the projection is restarted.

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## at-most-once

The offset for each envelope is stored before the envelope has been processed and giving at-most-once
processing semantics. This means that if the projection is restarted from previously stored offset one envelope
may not have been processed.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #projection-imports #atLeastOnce }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #projection-imports #atLeastOnce }

Since the offset must be stored for each envelope this is slower than @ref:[at-least-once](#at-least-once), which
can batch offsets before storing.

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## Grouping

The envelopes can be grouped before processing, which can be useful for batch updates.

TODO: Implementation in progress, see [PR #118](https://github.com/akka/akka-projection/pull/118)

## Handler

It's in the @apidoc[Handler] that you implement the processing of each envelope. It's essentially a function
from `Envelope` to @scala[`Future[Done]`]@java[`CompletionStage<Done>`]. This means that the envelope handler
can integrate with anything, such as publishing to a message broker, or updating a read model in Cassandra.

A handler that is consuming `ShoppingCart.Event` from `eventsByTag` can look like this:

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #handler-imports #handler }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #handler-imports #handler }

Such simple handlers can also be defined as plain @scala[functions]@java[lambdas] via the helper
@scala[`Handler.apply`]@java[`Handler.fromFunction`] factory method.

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
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #todo }

However, the state must typically be loaded and updated by asynchronous operations and then it can be
error prone to manage the state in variables of the `Handler`. For that purpose a @apidoc[StatefulHandler]
is provided.

Let us look at how a `StatefulHandler` can be implemented in the context of a "word count" domain. The purpose is
to process a stream of words and for each word keep track of how many times it has occurred. 

Given an envelope and `SourceProvider` for this example:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #envelope #sourceProvider }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #todo }

and a repository for the interaction with the database:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #repository }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #todo }

The `Projection` can be definined as:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/akka/projection/cassandra/scaladsl/WordCountDocExampleSpec.scala) { #projection }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #todo }

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
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #todo }

The `StatefulHandler` has two methods that needs to be implemented. 

* `initialState` - Invoked to load the initial state when the projection is started or if previous `process` failed.
* `process(state, envelope)` - Invoked for each `Envelope`, one at a time. The `state` parameter is the completed
  value of the previously returned `Future[State]` or the `initialState`.

If the previously returned `Future[State]` failed it will call `initialState` again and use that value.

Another implementation would be a handler that is loading the current count for a word on demand, and thereafter
caches it in the in-memory state:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #loadingOnDemand }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #todo }

### Handler as an actor

A good alternative for advanced state management is to implement the handler as an [actor](https://doc.akka.io/docs/akka/current/typed/actors.html).
 
TODO: Documentation pending, see [PR #116](https://github.com/akka/akka-projection/pull/116)


## Processing with Akka Streams

An Akka Streams `Flow` can be used instead of a handler for processing the envelopes with at-least-once semantics.

TODO: Implementation in progress, see [PR #119](https://github.com/akka/akka-projection/pull/119)

## Schema

```
CREATE TABLE IF NOT EXISTS akka_projection.offset_store (
  projection_id text,
  offset text,
  manifest text,
  last_updated timestamp,
  PRIMARY KEY (projection_id))
```

## Offset types

The supported offset types of the `CassandraProjection` are:

* `akka.persistence.query.Offset` types from @ref:[events from Akka Persistence](eventsourced.md)
* `String`
* @scala[`Int`]@java[Integer]
* `Long`

@@@ note

The @apidoc[MergeableOffset] that is used for @ref:[messages from Kafka](kafka.md) is not implemented
for the `CassandraProjection` yet, see [issue #97](https://github.com/akka/akka-projection/issues/97).

@@@

## Configuration

@@snip [reference.conf](/akka-projection-cassandra/src/main/resources/reference.conf)
