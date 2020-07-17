# Offset in a relational DB with Slick

The @apidoc[SlickProjection] has support for storing the offset in a relational database using
[Slick](http://scala-slick.org) (JDBC). This is only an option for Scala and for Java the
@ref:[offset can be stored in relational DB with JDBC](jdbc.md).

The source of the envelopes can be @ref:[events from Akka Persistence](eventsourced.md) or any other `SourceProvider`
with supported @ref:[offset types](#offset-types).

The envelope handler returns a `DBIO` that will be run by the projection. This means that the target database
operations can be run in the same transaction as the storage of the offset, which means that @ref:[exactly-once](#exactly-once)
processing semantics is supported. It also offers @ref:[at-least-once](#at-least-once) semantics.

@@@ note
Prefer using the JDBC implementation to implement your projection handler. Slick support in `akka-projection` is meant for users 
migrating from [`Lagom's Slick ReadSideProcessor`](https://www.lagomframework.com/documentation/1.6.x/scala/ReadSideSlick.html).
@@@

## Dependencies

To use the Slick module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
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

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## at-least-once

The offset is stored after the envelope has been processed and giving at-least-once processing semantics.
This means that if the projection is restarted from a previously stored offset some elements may be processed more
than once.

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #atLeastOnce }

The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
This window can be defined with `withSaveOffset` of the returned `AtLeastOnceProjection`.
The default settings for the window is defined in configuration section `akka.projection.at-least-once`.
There is a performance benefit of not storing the offset too often, but the drawback is that there can be more
duplicates when the projection that will be processed again when the projection is restarted.

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## groupedWithin

The envelopes can be grouped before processing, which can be useful for batch updates.

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #grouped }

The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first.
This window can be defined with `withGroup` of the returned `GroupedProjection`. The default settings for
the window is defined in configuration section `akka.projection.grouped`.

When using `groupedWithin` the handler is a `SlickHandler[immutable.Seq[EventEnvelope[ShoppingCart.Event]]]`.
The @ref:[`GroupedShoppingCartHandler` is shown below](#grouped-handler).

The offset is stored in the same transaction as the `DBIO` returned from the `handler`, which means exactly-once
processing semantics if the projection is restarted from previously stored offset.

## Handler

It's in the @apidoc[SlickHandler] that you implement the processing of each envelope. It's essentially a function
from `Envelope` to `DBIO[Done]`. The returned `DBIO` is run by the projection.

A handler that is consuming `ShoppingCart.Event` from `eventsByTag` can look like this:

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #handler-imports #handler }

@@@ note { title=Hint }
Such simple handlers can also be defined as plain functions via the helper `SlickHandler.apply` factory method.
@@@

where the `OrderRepository` is:

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #repository }

with the Slick `DatabaseConfig`:

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #db-config }


### Grouped handler

When using @ref:[`SlickProjection.groupedWithin`](#groupedwithin) the handler is processing a `Seq` of envelopes.

Scala
:  @@snip [SlickProjectionDocExample.scala](/examples/src/test/scala/docs/slick/SlickProjectionDocExample.scala) { #grouped-handler }

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

### Async handler

The @apidoc[Handler] can be used with `SlickProjection.atLeastOnceAsync` and 
`SlickProjection.groupedWithinAsync` if the handler is not storing the projection result in the database.
The handler could @ref:[send to a Kafka topic](kafka.md#sending-to-kafka) or integrate with something else.

There are several examples of such `Handler` in the @ref:[documentation for Cassandra Projections](cassandra.md#handler).
Same type of handlers can be used with `SlickProjection` instead of `CassandraProjection`.

### Actor handler

A good alternative for advanced state management is to implement the handler as an [actor](https://doc.akka.io/docs/akka/current/typed/actors.html),
which is described in @ref:[Processing with Actor](actor.md).

### Flow handler

An Akka Streams `FlowWithContext` can be used instead of a handler for processing the envelopes,
which is described in @ref:[Processing with Akka Streams](flow.md).

### Handler lifecycle

You can override the `start` and `stop` methods of the @apidoc[SlickHandler] to implement initialization
before first envelope is processed and resource cleanup when the projection is stopped.
Those methods are also called when the `Projection` is restarted after failure.

See also @ref:[error handling](error.md).

## Schema

The database schema for the offset storage table:

PostgreSQL
:  @@snip [create-table-default.sql](/examples/src/test/resources/create-table-default.sql) { #create-table-default }

MySQL
:  @@snip [create-table-mysql.sql](/examples/src/test/resources/create-table-mysql.sql) { #create-table-mysql }

Microsoft SQL Server
:  @@snip [create-table-mssql.sql](/examples/src/test/resources/create-table-mssql.sql) { #create-table-mssql }

Oracle
:  @@snip [create-table-oracle.sql](/examples/src/test/resources/create-table-oracle.sql) { #create-table-oracle }

H2
:  @@snip [create-table-default.sql](/examples/src/test/resources/create-table-default.sql) { #create-table-default }


## Offset types

The supported offset types of the `SlickProjection` are:

* `akka.persistence.query.Offset` types from @ref:[events from Akka Persistence](eventsourced.md)
* `MergeableOffset` that is used for @ref:[messages from Kafka](kafka.md)
* `String`
* `Int`
* `Long`
* Any other type that has a configured Akka Serializer is stored with base64 encoding of the serialized bytes.
  For example the [Akka Persistence Spanner](https://doc.akka.io/docs/akka-persistence-spanner/current/) offset
  is supported in this way.

## Configuration

Make your edits/overrides in your application.conf.

The reference configuration file with the default values:

@@snip [reference.conf](/akka-projection-slick/src/main/resources/reference.conf) { #config }
