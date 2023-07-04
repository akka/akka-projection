# Events from Akka Persistence

A typical source for Projections is events stored with @apidoc[EventSourcedBehavior$] in [Akka Persistence](https://doc.akka.io/docs/akka/current/typed/persistence.html). Events can be [tagged](https://doc.akka.io/docs/akka/current/typed/persistence.html#tagging) and then
consumed with the [eventsByTag query](https://doc.akka.io/docs/akka/current/persistence-query.html#eventsbytag-and-currenteventsbytag).

Akka Projections has integration with `eventsByTag`, which is described here. 

## Dependencies

To use the Event Sourced module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-eventsourced_$scala.binary.version$
  version=$project.version$
}

Akka Projections require Akka $akka.version$ or later, see @ref:[Akka version](overview.md#akka-version).

@@project-info{ projectId="akka-projection-eventsourced" }

### Transitive dependencies

The table below shows `akka-projection-eventsourced`'s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-eventsourced" }

## SourceProvider for eventsByTag

A @apidoc[SourceProvider] defines the source of the event envelopes that the `Projection` will process. A `SourceProvider`
for the `eventsByTag` query can be defined with the @apidoc[EventSourcedProvider$] like this:

Scala
:  @@snip [EventSourcedDocExample.scala](/examples/src/test/scala/docs/eventsourced/EventSourcedDocExample.scala) { #eventsByTagSourceProvider }

Java
:  @@snip [EventSourcedDocExample.java](/examples/src/test/java/jdocs/eventsourced/EventSourcedDocExample.java) { #eventsByTagSourceProvider }

This example is using the [Cassandra plugin for Akka Persistence](https://doc.akka.io/docs/akka-persistence-cassandra/current/read-journal.html),
but same code can be used for other Akka Persistence plugins by replacing the `CassandraReadJournal.Identifier`.
For example the [JDBC plugin](https://doc.akka.io/docs/akka-persistence-jdbc/current/) can be used. You will
use the same plugin as you have configured for the write side that is used by the `EventSourcedBehavior`.

This source is consuming all events from the `ShoppingCart` `EventSourcedBehavior` that are tagged with `"cart-1"`.

The tags are assigned as described in @ref:[Tagging Events in EventSourcedBehavior](running.md#tagging-events-in-eventsourcedbehavior).

The @scala[`EventEnvelope[ShoppingCart.Event]`]@java[`EventEnvelope<ShoppingCart.Event>`] is what the `Projection`
handler will process. It contains the `Event` and additional meta data, such as the offset that will be stored
by the `Projection`. See @apidoc[akka.projection.eventsourced.EventEnvelope] for full details of what the
envelope contains. 

## SourceProvider for eventsBySlices

A @apidoc[SourceProvider] defines the source of the event envelopes that the `Projection` will process. A `SourceProvider`
for the `eventsBySlices` query can be defined with the @apidoc[EventSourcedProvider$] like this:

Scala
:  @@snip [EventSourcedDocExample.scala](/examples/src/test/scala/docs/eventsourced/EventSourcedDocExample.scala) { #eventsBySlicesSourceProvider }

Java
:  @@snip [EventSourcedDocExample.java](/examples/src/test/java/jdocs/eventsourced/EventSourcedBySlicesDocExample.java) { #eventsBySlicesSourceProvider }

This example is using the @extref:[R2DBC plugin for Akka Persistence](akka-persistence-r2dbc:query.html).
You will use the same plugin as you have configured for the write side that is used by the `EventSourcedBehavior`.

This source is consuming all events from the `ShoppingCart` `EventSourcedBehavior` for the given slice range. In a production application, you would need to start as many instances as the number of slice ranges. That way you consume the events from all entities.

The @scala[`EventEnvelope[ShoppingCart.Event]`]@java[`EventEnvelope<ShoppingCart.Event>`] is what the `Projection`
handler will process. It contains the `Event` and additional meta data, such as the offset that will be stored
by the `Projection`. See @apidoc[akka.persistence.query.typed.EventEnvelope] for full details of what the
envelope contains.

## SourceProvider for eventsBySlicesStartingFromSnapshots

The Projection can use snapshots as starting points and thereby reducing number of events that have to be loaded.
This can be useful if the consumer start from zero without any previously processed offset or if it has been
disconnected for a long while and its offset is far behind.

You need to define the snapshot to event transformation function in `EventSourcedProvider.eventsBySlicesStartingFromSnapshots`.

The underlying read journal must implement @apidoc[EventsBySliceStartingFromSnapshotsQuery].
See how to enable @extref:[eventsBySlicesStartingFromSnapshots in Akka Persistence R2DBC](akka-persistence-r2dbc:query.html#eventsbyslicesstartingfromsnapshots).

## Many Projections 

@apidoc[akka.persistence.query.typed.*.EventsBySliceFirehoseQuery] can give better scalability when many
consumers retrieve the same events, for example many Projections of the same entity type. The purpose is
to share the stream of events from the database and fan out to connected consumer streams. Thereby fewer queries
and loading of events from the database.

`EventsBySliceFirehoseQuery` is used in place of `EventsBySliceQuery` with the `EventSourcedProvider`.

It is typically used together with @extref:[Sharded Daemon Process with colocated processes](akka:typed/cluster-sharded-daemon-process.md#colocate-processes).
