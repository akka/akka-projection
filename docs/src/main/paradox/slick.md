# Offset in relational DB with Slick

The @apidoc[SlickProjection] has support for storing the offset in a relational database with
[Slick](http://scala-slick.org) (JDBC). This is only an option for Scala and for Java the
@ref:[offset can be stored in relational DB with JPA](jpa.md).

The source of the envelopes can be @ref:[events from Akka Persistence](eventsourced.md) or any other `SourceProvider`
with supported @ref:[offset types](#offset-types).

The envelope handler returns a `DBIO` that will be run by the projection. This means that the target database
operations can be run in the same transaction as the storage of the offset, which means that @ref:[exactly-once](#exactly-once)
processing semantics is supported. It also offers @ref:[at-least-once](#at-least-once) semantics.

## Dependencies

To use the Slick module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.typesafe.akka
  artifact=akka-projection-slick_$scala.binary.version$
  version=$project.version$
}

Akka Projections require Akka $akka.version$ or later, see @ref:[Akka version](overview.md#akka-version).

@@project-info{ projectId="akka-projection-slick" }

### Transitive dependencies

The table below shows `akka-projection-slick`'s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-slick" }

## exactly-once

The offset is stored in the same transaction as the `DBIO` returned from the `handler`, which means exactly-once
processing semantics if the projection is restarted from previously stored offset.

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #projection-imports #exactlyOnce }

## at-least-once

The offset is stored after the envelope has been processed and giving at-least-once processing semantics.
This means that if the projection is restarted from a previously stored offset some elements may be processed more
than once.

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #atLeastOnce }

The `saveOffsetAfterEnvelopes` and `saveOffsetAfterDuration` parameters control how often the offset is stored.
There is a performance benefit of not storing the offset too often but the drawback is that there can be more
duplicates when the projection that will be processed again when the projection is restarted.

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## Grouping

The envelopes can be grouped before processing, which can be useful for batch updates.

TODO: Implementation in progress, see [PR #118](https://github.com/akka/akka-projection/pull/118)

## Handler

It's in the @apidoc[SlickHandler] that you implement the processing of each envelope. It's essentially a function
from `Envelope` to `DBIO[Done]`. The returned `DBIO` is run by the projection.

A handler that is consuming `ShoppingCart.Event` from `eventsByTag` can look like this:

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #handler-imports #handler }

where the `OrderRepository` is:

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #repository }

with the Slick `DatabaseConfig`:

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #db-config }

Such simple handlers can also be defined as plain functions via the helper `SlickHandler.apply` factory method.

### Stateful handler

The `SlickHandler` can be stateful, with variables and mutable data structures. It is invoked by the `Projection` machinery
one envelope at a time and visibility guarantees between the invocations are handled automatically, i.e. no volatile
or other concurrency primitives are needed for managing the state as long as it's not accessed by other threads
than the one that called `process`.

@@@ note

It is important that the `Handler` instance is not shared between several `Projection` instances,
because then it would be invoked concurrently, which is not how it is intended to be used. Each `Projection`
instance should use a new `Handler` instance.  

@@@

### Handler lifecycle

You can override the `start` and `stop` methods of the @apidoc[SlickHandler] to implement initialization
before first envelope is processed and resource cleanup when the projection is stopped.
Those methods are also called when the `Projection` is restarted after failure.

## Schema

The database schema for the offset storage table:

```
create table if not exists "AKKA_PROJECTION_OFFSET_STORE" (
  "PROJECTION_NAME" CHAR(255) NOT NULL,
  "PROJECTION_KEY" CHAR(255) NOT NULL,
  "OFFSET" CHAR(255) NOT NULL,
  "MANIFEST" VARCHAR(4) NOT NULL,
  "MERGEABLE" BOOLEAN NOT NULL,
  "LAST_UPDATED" TIMESTAMP(9) WITH TIME ZONE NOT NULL);

alter table "AKKA_PROJECTION_OFFSET_STORE" add constraint "PK_PROJECTION_ID" primary key("PROJECTION_NAME","PROJECTION_KEY");
```

## Offset types

The supported offset types of the `SlickProjection` are:

* `akka.persistence.query.Offset` types from @ref:[events from Akka Persistence](eventsourced.md)
* `MergeableOffset` that is used for @ref:[messages from Kafka](kafka.md)
* `String`
* @scala[`Int`]@java[Integer]
* `Long`

## Configuration

@@snip [reference.conf](/akka-projection-slick/src/main/resources/reference.conf)
