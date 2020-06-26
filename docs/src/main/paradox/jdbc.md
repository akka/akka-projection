# Offset in a relational DB with JDBC

The @apidoc[JdbcProjection] has support for storing the offset in a relational database using JDBC.

The source of the envelopes can be @ref:[events from Akka Persistence](eventsourced.md) or any other `SourceProvider`
with supported @ref:[offset types](#offset-types).

A @apidoc[JdbcHandler] receives a @apidoc[JdbcSession] instance and an envelope. The `JdbcSession` provides the means to access an open JDBC connection that can be used to process the envelope. The target database operations can be run in the same transaction as the storage of the offset, which means that @ref:[exactly-once](#exactly-once)
processing semantics is supported. It also offers @ref:[at-least-once](#at-least-once) semantics.

## Dependencies

To use the JDBC module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-jdbc_$scala.binary.version$
  version=$project.version$
}

Akka Projections require Akka $akka.version$ or later, see @ref:[Akka version](overview.md#akka-version).

@@project-info{ projectId="akka-projection-jdbc" }


### Transitive dependencies

The table below shows `akka-projection-jdbc`'s direct dependencies, and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-jdbc" }

## Required configuration settings

There are two settings that need to be set beforehand in your `application.conf` file.

* `akka.projection.jdbc.dialect` - The dialect type indicating your database of choice. Supported dialects are: `mysql-dialect`, `postgres-dialect`, `mssql-dialect` or `h2-dialect` (testing).
* `akka.projection.jdbc.blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size` indicating the size of the blocking JDBC dispatcher. See also @ref:[Blocking JDBC Dispatcher](#blocking-jdbc-dispatcher).

## Defining a JdbcSession

Before using Akka Projection JDBC you must implement a `JdbcSession` @scala[trait]@java[interface]. `JdbcSession` is used to open a connection and start a transaction. A new `JdbcSession` will be created for each call to the handler. At the end of the processing, the transaction will be committed (or rolled back). 

When using `JdbcProjection.exactlyOnce`, the `JdbcSession` that is passed to the handler will be used to save the offset behind the scenes. Therefore, it's extremely important to disable auto-commit (eg: `setAutoCommit(false)`), otherwise the two operations won't participate on the same transaction.  

Scala
:  @@snip [JdbcProjectionDocExample.scala](/examples/src/test/scala/docs/jdbc/JdbcProjectionDocExample.scala) { #jdbc-session-imports #jdbc-session }

Java
:  @@snip [JdbcProjectionDocExample.java](/examples/src/test/java/jdocs/jdbc/JdbcProjectionDocExample.java) { #jdbc-session-imports #jdbc-session }


@@@ note
It's highly recommended configuring it with a connection pool, for example [HikariCP](https://github.com/brettwooldridge/HikariCP).
@@@

When declaring a `JdbcProjection` you must provide a factory for the `JdbcSession`. The factory will be used to create new instances whenever needed.

An alternative Hibernate based implementation would look like this:

Java
:  @@snip [HibernateJdbcSession.java](/examples/src/test/java/jdocs/jdbc/HibernateJdbcSession.java) { #hibernate-session-imports #hibernate-session } 

And a special factory that initializes the `EntityManagerFactory` and builds the `JdbcSession` instance:

Java
:  @@snip [HibernateSessionFactory.java](/examples/src/test/java/jdocs/jdbc/HibernateSessionFactory.java) { #hibernate-factory-imports #hibernate-factory }


## Blocking JDBC Dispatcher

JDBC APIs are blocking by design, therefore Akka Projection JDBC will use a dedicated dispatcher to run all JDBC calls. It's important to configure the dispatcher to have the same size as the connection pool. 

Each time the projection handler is called one thread and one database connection will be used. If your connection pool is smaller than the number of threads, the thread can potentially block while waiting for the connection pool to provide a connection. 

The dispatcher pool size can be configured through the `akka.projection.jdbc.blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size` settings. See @ref:[Configuration](#configuration) section below.

@@@ note
Most applications will use database connections to read data, for instance to read a projected model upon user request. This means that other parts of the application will be competing for a connection. It's recommend to configure a connection pool dedicated to the projections and use a different one in other parts of the application.  
@@@

## exactly-once

The offset is stored in the same transaction used for the user defined `handler`, which means exactly-once
processing semantics if the projection is restarted from previously stored offset.

Scala
:  @@snip [JdbcProjectionDocExample.scala](/examples/src/test/scala/docs/jdbc/JdbcProjectionDocExample.scala) { #projection-imports #exactlyOnce }

Java
:  @@snip [JdbcProjectionDocExample.java](/examples/src/test/java/jdocs/jdbc/JdbcProjectionDocExample.java) { #exactlyOnce }

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## at-least-once

The offset is stored after the envelope has been processed and giving at-least-once processing semantics.
This means that if the projection is restarted from a previously stored offset some elements may be processed more
than once.

Scala
:  @@snip [JdbcProjectionDocExample.scala](/examples/src/test/scala/docs/jdbc/JdbcProjectionDocExample.scala) { #projection-imports #atLeastOnce }

Java
:  @@snip [JdbcProjectionDocExample.java](/examples/src/test/java/jdocs/jdbc/JdbcProjectionDocExample.java) { #atLeastOnce }

The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
This window can be defined with `withSaveOffset` of the returned `AtLeastOnceProjection`.
The default settings for the window is defined in configuration section `akka.projection.at-least-once`.
There is a performance benefit of not storing the offset too often, but the drawback is that there can be more
duplicates when the projection that will be processed again when the projection is restarted.

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## groupedWithin

The envelopes can be grouped before processing, which can be useful for batch updates.

Scala
:  @@snip [JdbcProjectionDocExample.scala](/examples/src/test/scala/docs/jdbc/JdbcProjectionDocExample.scala) { #projection-imports #grouped }

Java
:  @@snip [JdbcProjectionDocExample.java](/examples/src/test/java/jdocs/jdbc/JdbcProjectionDocExample.java) { #grouped }

The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first.
This window can be defined with `withGroup` of the returned `GroupedProjection`. The default settings for
the window is defined in configuration section `akka.projection.grouped`.

When using `groupedWithin` the handler is a @scala[`JdbcHandler[immutable.Seq[EventEnvelope[ShoppingCart.Event]]]`]@java[`JdbcHandler<List<EventEnvelope<ShoppingCart.Event>>>`].
The @ref:[`GroupedShoppingCartHandler` is shown below](#grouped-handler).

The offset is stored in the same transaction used for the user defined `handler`, which means exactly-once
processing semantics if the projection is restarted from previously stored offset.

## Handler

It's in the @apidoc[JdbcHandler] that you implement the processing of each envelope. It's essentially a consumer function
from `(JdbcSession, Envelope)` to @scala[`Unit`]@java[`void`]. 

A handler that is consuming `ShoppingCart.Event` from `eventsByTag` can look like this:

Scala
:  @@snip [JdbcProjectionDocExample.scala](/examples/src/test/scala/docs/jdbc/JdbcProjectionDocExample.scala) { #handler-imports #handler }

Java
:  @@snip [JdbcProjectionDocExample.java](/examples/src/test/java/jdocs/jdbc/JdbcProjectionDocExample.java) { #handler }

@@@ note { title=Hint }
Such simple handlers can also be defined as plain functions via the helper @scala[`JdbcHandler.apply`]@java[`JdbcHandler.fromFunction`] factory method.
@@@

where the `OrderRepository` is an implementation of:

Scala
:  @@snip [JdbcProjectionDocExample.scala](/examples/src/test/scala/docs/jdbc/JdbcProjectionDocExample.scala) { #repository }

Java
:  @@snip [JdbcProjectionDocExample.java](/examples/src/test/java/jdocs/jdbc/JdbcProjectionDocExample.java) { #repository }

### Grouped handler

When using @ref:[`JdbcProjection.groupedWithin`](#groupedwithin) the handler is processing a @scala[`Seq`]@java[`List`] of envelopes.

Scala
:  @@snip [JdbcProjectionDocExample.scala](/examples/src/test/scala/docs/jdbc/JdbcProjectionDocExample.scala) { #handler-imports  #grouped-handler }

Java
:  @@snip [JdbcProjectionDocExample.java](/examples/src/test/java/jdocs/jdbc/JdbcProjectionDocExample.java) { #grouped-handler }

### Stateful handler

The `JdbcHandler` can be stateful, with variables and mutable data structures. It is invoked by the `Projection` machinery
one envelope at a time and visibility guarantees between the invocations are handled automatically, i.e. no volatile
or other concurrency primitives are needed for managing the state as long as it's not accessed by other threads
than the one that called `process`.

@@@ note

It is important that the `Handler` instance is not shared between several `Projection` instances,
because then it would be invoked concurrently, which is not how it is intended to be used. Each `Projection`
instance should use a new `Handler` instance.  

@@@

### Async handler

The @apidoc[Handler] can be used with `JdbcProjection.atLeastOnceAsync` and 
`JdbcProjection.groupedWithinAsync` if the handler is not storing the projection result in the database.
The handler could @ref:[send to a Kafka topic](kafka.md#sending-to-kafka) or integrate with something else.

There are several examples of such `Handler` in the @ref:[documentation for Cassandra Projections](cassandra.md#handler).
Same type of handlers can be used with `JdbcProjection` instead of `CassandraProjection`.

### Actor handler

A good alternative for advanced state management is to implement the handler as an [actor](https://doc.akka.io/docs/akka/current/typed/actors.html),
which is described in @ref:[Processing with Actor](actor.md).

### Flow handler

An Akka Streams `FlowWithContext` can be used instead of a handler for processing the envelopes,
which is described in @ref:[Processing with Akka Streams](flow.md).

### Handler lifecycle

You can override the `start` and `stop` methods of the @apidoc[JdbcHandler] to implement initialization
before first envelope is processed and resource cleanup when the projection is stopped.
Those methods are also called when the `Projection` is restarted after failure.

See also @ref:[error handling](error.md).

## Schema

The database schema for the offset storage table:

Postgres
:  @@snip [create-tables.sql](/examples/src/test/resources/create-tables.sql) { #create-table-default }

MySQL
:  @@snip [create-tables.sql](/examples/src/test/resources/create-tables.sql) { #create-table-mysql }

MS SQL Server
:  @@snip [create-tables.sql](/examples/src/test/resources/create-tables.sql) { #create-table-mssql }

H2
:  @@snip [create-tables.sql](/examples/src/test/resources/create-tables.sql) { #create-table-default }



## Offset types

The supported offset types of the `JdbcProjection` are:

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

@@snip [reference.conf](/akka-projection-jdbc/src/main/resources/reference.conf) { #config }

@@@ note
Settings `akka.projection.jdbc.dialect` and `akka.projection.jdbc.blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size` do not have a valid default value. You must configured them in your `application.conf` file.  

See @ref:[Required Configuration Settings](#required-configuration-settings) and @ref:[Blocking JDBC Dispatcher](#blocking-jdbc-dispatcher) sections for details. 
@@@