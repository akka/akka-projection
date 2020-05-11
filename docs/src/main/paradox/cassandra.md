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

Akka Projections require Akka $akka.version$ or later, see @ref:[Akka version](overview.md#akka-version).

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

TODO stateful handlers...

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
