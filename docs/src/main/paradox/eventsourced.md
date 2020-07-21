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
:  @@snip [EventSourcedDocExample.scala](/examples/src/test/scala/docs/eventsourced/EventSourcedDocExample.scala) { #imports #sourceProvider }

Java
:  @@snip [EventSourcedDocExample.java](/examples/src/test/java/jdocs/eventsourced/EventSourcedDocExample.java) { #imports #sourceProvider }

This example is using the [Cassandra plugin for Akka Persistence](https://doc.akka.io/docs/akka-persistence-cassandra/current/read-journal.html),
but same code can be used for other Akka Persistence plugins by replacing the `CassandraReadJournal.Identifier`.
For example the [JDBC plugin](https://doc.akka.io/docs/akka-persistence-jdbc/current/) can be used. You will
use the same plugin as you have configured for the write side that is used by the `EventSourcedBehavior`.

This source is consuming all events from the `ShoppingCart` `EventSourcedBehavior` that are tagged with `"cart-1"`.

The @scala[`EventEnvelope[ShoppingCart.Event]`]@java[`EventEnvelope<ShoppingCart.Event>`] is what the `Projection`
handler will process. It contains the `Event` and additional meta data, such as the offset that will be stored
by the `Projection`. See @apidoc[akka.projection.eventsourced.EventEnvelope] for full details of what the
envelope contains. 
