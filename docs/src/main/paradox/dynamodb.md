# Offset in DynamoDB

@apidoc[DynamoDBProjection$] has support for storing offsets in DynamoDB via @extref:[Akka Persistence DynamoDB](akka-persistence-dynamodb:overview.html).

The source of the envelopes is from a `SourceProvider`, which can be:

* events from Event Sourced entities via the @ref:[SourceProvider for eventsBySlices](eventsourced.md#sourceprovider-for-eventsbyslices) with the @extref:[eventsBySlices query](akka-persistence-dynamodb:query.html#eventsbyslices)

The target database operations can run in the same transaction as storing the offset, so that
@ref:[exactly-once](#exactly-once) processing semantics are supported. It also offers
@ref:[at-least-once](#at-least-once) semantics.

## Dependencies

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [Maven,sbt,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

To use the DynamoDB module of Akka Projections, add the following dependencies to your project:

@@dependency [Maven,sbt,Gradle] {
group=com.lightbend.akka
artifact=akka-projection-dynamodb_$scala.binary.version$
version=$project.version$
group2=com.lightbend.akka
artifact2=akka-persistence-dynamodb_$scala.binary.version$
version2=$project.version$
}

Akka Projection DynamoDB depends on Akka $akka.version$ or later, and note that it is important that all `akka-*`
dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems with
transient dependencies causing an unlucky mix of versions.

@@project-info{ projectId="akka-projection-dynamodb" }

### Transitive dependencies

The table below shows `akka-projection-dynamodb`'s direct dependencies, and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-dynamodb" }

## Tables

Akka Projection DynamoDB requires an offset table to be created in DynamoDB. The default table name is
`timestamp_offset` and this can be configured (see the @ref:[reference configuration](#reference-configuration) for all
settings). The table should be created with the following attributes and key schema:

| Attribute name | Attribute type | Key type |
| -------------- | -------------- | -------- |
| name_slice     | S (String)     | HASH     |
| pid            | S (String)     | RANGE    |

Read and write capacity units should be based on expected projection activity.

An example `aws` CLI command for creating the timestamp offset table:

@@snip [aws create timestamp offset table](/akka-projection-dynamodb/scripts/create-tables.sh) { #create-timestamp-offset-table }

### Creating tables locally

The DynamoDB client @extref:[can be configured](akka-persistence-dynamodb:config.html#dynamodb-client-configuration)
with a local mode, for testing with DynamoDB local:

@@snip [local mode](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #local-mode }

Similar to @extref:[creating tables locally](akka-persistence-dynamodb:getting-started.html#creating-tables-locally)
for Akka Persistence DynamoDB, a @apidoc[akka.projection.dynamodb.*.CreateTables$] utility is provided for creating
projection tables locally:

Java
: @@snip [create tables](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #create-tables }

Scala
: @@snip [create tables](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #create-tables }

## Configuration

By default, Akka Projection DynamoDB shares the @extref:[DynamoDB client configuration](akka-persistence-dynamodb:config.html#dynamodb-client-configuration) with Akka Persistence DynamoDB.

### Batch writes

Offsets are written in batches for @ref:[at-least-once](#at-least-once) and @ref:[at-least-once (grouped)](#at-least-once-grouped-).
To reduce the risk of write throttling it is recommended to save at most 25 offsets at a time. This is configured by:  

```hcon
akka.projection.at-least-once.save-offset-after-envelopes = 25
akka.projection.grouped.group-after-envelopes = 25
```

### Reference configuration

The following can be overridden in your `application.conf` for projection specific settings:

@@snip [reference.conf](/akka-projection-dynamodb/src/main/resources/reference.conf) { #projection-config }

## Running with Sharded Daemon Process

The Sharded Daemon Process can be used to distribute `n` instances of a given Projection across the cluster.
Therefore, it's important that each Projection instance consumes a subset of the stream of envelopes.

When using `eventsBySlices` the initialization code looks like this:

Java
:  @@snip [init projections](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #init-projections }

Scala
:  @@snip [init projections](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #init-projections }

The @ref:[`ShoppingCartTransactHandler` is shown below](#transact-handler).

It is possible to dynamically scale the number of Projection instances as described in the @extref:[Sharded Daemon
Process documentation](akka:typed/cluster-sharded-daemon-process.html#dynamic-scaling-of-number-of-workers).

There are alternative ways of running the `ProjectionBehavior` as described in @ref:[Running a Projection](running.md).

## Slices

The `SourceProvider` for Event Sourced actors has historically been using `eventsByTag` but the DynamoDB plugin is
instead providing `eventsBySlices` as an improved solution.

The usage of `eventsByTag` for Projections has the drawback that the number of tags must be decided up-front and can't
easily be changed afterwards. Starting with too many tags means a lot of overhead, since many projection instances
would be running on each node in a small Akka Cluster, with each projection instance polling the database periodically.
Starting with too few tags means that it can't be scaled later to more Akka nodes.

With `eventsBySlices` more Projection instances can be added when needed and still reuse the offsets for the previous
slice distributions.

A slice is deterministically defined based on the persistence id. The purpose is to evenly distribute all
persistence ids over the slices. The `eventsBySlices` query is for a range of the slices. For example if
using 1024 slices and running 4 Projection instances the slice ranges would be 0-255, 256-511, 512-767, 768-1023.
Changing to 8 slice ranges means that the ranges would be 0-127, 128-255, 256-383, ..., 768-895, 896-1023.

However, when changing the number of slices the projections with the old slice distribution must be stopped before
starting new projections. That can be done at runtime when @ref:[Running with Sharded Daemon
Process](#running-with-sharded-daemon-process).

When using `DynamoDBProjection` together with the `EventSourcedProvider.eventsBySlices` the events will be delivered in
sequence number order without duplicates.

## exactly-once

The offset is stored in the same transaction as items returned by the projection handler, providing exactly-once
processing semantics if the projection is restarted from a previously stored offset. A @apidoc[DynamoDBTransactHandler]
is implemented, returning a collection of DynamoDB `TransactWriteItem`s which will be stored in the same transaction.

Java
:  @@snip [exactly once](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #projection-imports #exactly-once }

Scala
:  @@snip [exactly once](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #exactly-once }

The @ref:[`ShoppingCartTransactHandler` is shown below](#transact-handler).

## at-least-once

The offset is stored after the envelope has been processed, providing at-least-once processing semantics. This means
that if the projection is restarted from a previously stored offset some elements may be processed more than once.
Therefore, the @ref:[Handler](#handler) code must be idempotent.

Java
:  @@snip [at least once](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #projection-imports #at-least-once }

Scala
:  @@snip [at least once](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #at-least-once }

The offset is stored after a time window, or limited by a number of envelopes, whatever happens first. This window can
be defined with `withSaveOffset` of the returned `AtLeastOnceProjection`. The default settings for the window is
defined in the configuration section `akka.projection.at-least-once`. There is a performance benefit of not storing the
offset too often, but the drawback is that there can be more duplicates that will be processed again when the
projection is restarted.

Offsets are written in batches. To reduce the risk of write throttling it is recommended to save at most 25 offsets at a time.

The @ref:[`ShoppingCartHandler` is shown below](#generic-handler).

## exactly-once (grouped)

The envelopes can be grouped before processing, which can be useful for batch updates.

The offset is stored in the same transaction as items returned by the projection handler, providing exactly-once
processing semantics if the projection is restarted from a previously stored offset. A @apidoc[DynamoDBTransactHandler]
is implemented, returning a collection of DynamoDB `TransactWriteItem`s which will be stored in the same transaction.

The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first. This
window can be defined using `withGroup` of the returned `GroupedProjection`. The default settings for the window is
defined in the configuration section `akka.projection.grouped`.

Java
:  @@snip [exactly once grouped within](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #projection-imports #exactly-once-grouped-within }

Scala
:  @@snip [exactly once grouped within](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #exactly-once-grouped-within }

The @ref:[`GroupedShoppingCartTransactHandler` is shown below](#grouped-transact-handler).

## at-least-once (grouped)

The envelopes can be grouped before processing, which can be useful for batch updates.

The offsets are stored after the envelopes have been processed, providing at-least-once processing semantics. This
means that if the projection is restarted from a previously stored offset some elements may be processed more than
once. Therefore, the @ref:[Handler](#handler) code must be idempotent.

The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first. This
window can be defined using `withGroup` of the returned `GroupedProjection`. The default settings for the window is
defined in the configuration section `akka.projection.grouped`.

Java
:  @@snip [at least once grouped within](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #projection-imports #at-least-once-grouped-within }

Scala
:  @@snip [at least once grouped within](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #at-least-once-grouped-within }

Offsets are written immediately after the group of envelopes has been processed. To reduce the risk of write throttling
it is recommended to save at most 25 offsets at a time, and therefore not exceed this for the group size.

The @ref:[`GroupedShoppingCartHandler` is shown below](#grouped-handler).

## Handler

For at-least-once processing, a generic projection @apidoc[Handler] is implemented and projections can do any
processing, with or without DynamoDB.

For exactly-once processing, a @apidoc[DynamoDBTransactHandler] is implemented, returning a collection of DynamoDB
`TransactWriteItem`s which will be written in the same transaction as storing the offsets.

### Generic handler

A generic @apidoc[Handler] that is consuming `ShoppingCart.Event` from `eventsBySlices` can look like this:

Java
:  @@snip [handler](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #handler }

Scala
:  @@snip [handler](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #handler }

@@@ note { title=Hint }
Simple handlers can also be defined as plain functions via the helper @scala[`Handler.apply`]@java[`Handler.fromFunction`] factory method.
@@@

### Transact handler

A @apidoc[DynamoDBTransactHandler] that is consuming `ShoppingCart.Event` from `eventsBySlices` can look like this:

Java
:  @@snip [transact handler](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #transact-handler }

Scala
:  @@snip [transact handler](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #transact-handler }

@@@ note { title=Hint }
Simple handlers can also be defined as plain functions via the helper @scala[`DynamoDBTransactHandler.apply`]@java[`DynamoDBTransactHandler.fromFunction`] factory method.
@@@

### Grouped handler

When using @ref:[`DynamoDBProjection.atLeastOnceGroupedWithin`](#at-least-once-grouped-) the handler is processing a @scala[`Seq`]@java[`List`] of envelopes.

If a [batch write](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html) to DynamoDB
is being used, it's possible for items in the batch to fail to be written, and the response should be checked for
unprocessed items. A @apidoc[akka.projection.dynamodb.*.Requests$] utility is provided, to retry batch writes (with
exponential backoff) for any unprocessed items.

Java
:  @@snip [grouped handler](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #grouped-handler }

Scala
:  @@snip [grouped handler](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #grouped-handler }

### Grouped transact handler

When using @ref:[`DynamoDBProjection.exactlyOnceGroupedWithin`](#exactly-once-grouped-) the
@apidoc[DynamoDBTransactHandler] is processing a @scala[`Seq`]@java[`List`] of envelopes.

Java
:  @@snip [grouped handler](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #grouped-transact-handler }

Scala
:  @@snip [grouped handler](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #grouped-transact-handler }

### Stateful handler

The @apidoc[Handler] or @apidoc[DynamoDBTransactHandler] can be stateful, with variables and mutable data structures.
It is invoked by the `Projection` machinery one envelope or group of envelopes at a time and visibility guarantees
between the invocations are handled automatically, i.e. no volatile or other concurrency primitives are needed for
managing the state as long as it's not accessed by other threads than the one that called `process`.

@@@ note
It is important that the handler instance is not shared between several projection instances, because then it would be
invoked concurrently, which is not how it is intended to be used. Each projection instance should use a new handler
instance.
@@@

### Flow handler

An Akka Streams `FlowWithContext` can be used instead of a handler for processing the envelopes, which is described in
@ref:[Processing with Akka Streams](flow.md).

In addition to the caveats described there a `DynamoDBProjection.atLeastOnceFlow` must not filter out envelopes. Always
emit a `Done` element for each completed envelope, even if application processing was skipped for the envelope.

### Handler lifecycle

You can override the `start` and `stop` methods of the @apidoc[Handler] or @apidoc[DynamoDBTransactHandler] to
implement initialization before the first envelope is processed and resource cleanup when the projection is stopped.
Those methods are also called when the projection is restarted after failure.

See also @ref:[error handling](error.md).

## Publish events for lower latency

See @extref:[eventsBySlices documentation](akka-persistence-dynamodb:query.html#publish-events-for-lower-latency-of-eventsbyslices).

## Multiple plugins

Similar to how multiple plugins can be configured for the @extref[DynamoDB persistence
plugin](akka-persistence-dynamodb:config.html#multiple-plugins), multiple projection configurations are also possible.

For the projection offset store you need an additional config section:

@@snip [second config](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #second-projection-config }

Note that the `use-client` property references the same client settings as used for the `second-dynamodb` plugins, but
it could also have been a separate client configured as:

@@snip [second config with client](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #second-projection-config-with-client }

In this way, you can use the default plugins for the write side and projection `SourceProvider`, but use a separate configuration for the projection handlers and offset storage.

You start the projections with @apidoc[DynamoDBProjectionSettings] loaded from `"second-projection-dynamodb"`.

Java
:  @@snip [projection settings](/akka-projection-dynamodb-integration/src/test/java/projection/docs/javadsl/ProjectionDocExample.java) { #projection-settings }

Scala
:  @@snip [projection settings](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/scaladsl/ProjectionDocExample.scala) { #projection-settings }

## Time to Live (TTL)

Offsets are never deleted by default. To have offsets deleted for inactive projections, an expiration timestamp can be
set. DynamoDB's [Time to Live (TTL)][ttl] feature can then be enabled, to automatically delete items after they have
expired. A new expiration timestamp will be set each time an offset for a particular projection slice or persistence id
is updated.

The TTL attribute to use for the timestamp offset table is named `expiry`.

Time-to-live settings are configured per projection. The projection name can also be matched by prefix by using a `*`
at the end of the key. For example, offsets can be configured to expire in 7 days for a particular projection, and in
14 days for all projection names that start with a particular prefix:

@@ snip [offset time-to-live](/akka-projection-dynamodb-integration/src/test/scala/projection/docs/config/ProjectionTimeToLiveSettingsDocExample.scala) { #time-to-live type=conf }

### Time to Live reference configuration

The following can be overridden in your `application.conf` for the time-to-live specific settings:

@@snip [reference.conf](/akka-projection-dynamodb/src/main/resources/reference.conf) { #time-to-live-settings }

[ttl]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html
