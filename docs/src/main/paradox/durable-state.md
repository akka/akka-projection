# Changes from Durable State

A typical source for Projections is the change stored with @apidoc[DurableStateBehavior$] in [Akka Persistence](https://doc.akka.io/libraries/akka-core/current/typed/durable-state/persistence.html). Durable state changes can be consumed in a Projection with
`changesByTag`, `changesBySlices` or `eventsBySlices` queries.

Note that NOT all changes that occur are guaranteed to be emitted, calls to these methods only guarantee that eventually, the most recent
change for each object will be emitted. In particular, multiple updates to a given object in quick
succession are likely to be skipped, with only the last update resulting in a change from this source.


@@@ note { title=Alternative }
When using the R2DBC plugin an alternative to using a Projection is to @extref:[store the query representation](akka-persistence-r2dbc:durable-state-store.html#storing-query-representation) directly from the write side.
@@@

## Dependencies

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

To use the Durable State module of Akka Projections, add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-durable-state_$scala.binary.version$
  version=$project.version$
}

Akka Projections requires Akka $akka.version$ or later, see @ref:[Akka version](overview.md#akka-version).

@@project-info{ projectId="akka-projection-durable-state" }

### Transitive dependencies

The table below shows the `akka-projection-durable-state` direct dependencies.The second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-durable-state" }

## SourceProvider for changesByTag

A @apidoc[SourceProvider] defines the source of the envelopes that the `Projection` will process. A `SourceProvider`
for the `changes` query can be defined with the @apidoc[DurableStateStoreProvider] like this:

Scala
:  @@snip [DurableStateStoreDocExample.scala](/examples/src/test/scala/docs/state/DurableStateStoreDocExample.scala) { #changesByTagSourceProvider }

Java
:  @@snip [DurableStateStoreDocExample.java](/examples/src/test/java/jdocs/state/DurableStateStoreDocExample.java) { #changesByTagSourceProvider }

This example is using the [DurableStateStore JDBC plugin for Akka Persistence](https://doc.akka.io/libraries/akka-persistence-jdbc/current/durable-state-store.html).
You will use the same plugin that you configured for the write side. The one that is used by the `DurableStateBehavior`.

This source is consuming all the changes from the `Account` `DurableStateBehavior` that are tagged with `"bank-accounts-1"`. In a production application, you would need to start as many instances as the number of different tags you used. That way you consume the changes from all entities.

The @scala[`DurableStateChange[AccountEntity.Account]`]@java[`DurableStateChange<AccountEntity.Account>`] is what the `Projection`
handler will process. It contains the `State` and additional meta data, such as the offset that will be stored
by the `Projection`. See @apidoc[akka.persistence.query.DurableStateChange] for full details of what it contains. 

## SourceProvider for changesBySlices

A @apidoc[SourceProvider] defines the source of the envelopes that the `Projection` will process. A `SourceProvider`
for the `changesBySlices` query can be defined with the @apidoc[DurableStateStoreProvider] like this:

Scala
:  @@snip [DurableStateStoreDocExample.scala](/examples/src/test/scala/docs/state/DurableStateStoreDocExample.scala) { #changesBySlicesSourceProvider }

Java
:  @@snip [DurableStateStoreDocExample.java](/examples/src/test/java/jdocs/state/DurableStateStoreBySlicesDocExample.java) { #changesBySlicesSourceProvider }

This example is using the @extref:[R2DBC plugin for Akka Persistence](akka-persistence-r2dbc:query.html).
You will use the same plugin that you configured for the write side. The one that is used by the `DurableStateBehavior`.

This source is consuming all the changes from the `Account` `DurableStateBehavior` for the given slice range. In a production application, you would need to start as many instances as the number of slice ranges. That way you consume the changes from all entities.

The @scala[`DurableStateChange[AccountEntity.Account]`]@java[`DurableStateChange<AccountEntity.Account>`] is what the `Projection`
handler will process. It contains the `State` and additional meta data, such as the offset that will be stored
by the `Projection`. See @apidoc[akka.persistence.query.DurableStateChange] for full details of what it contains. 

## SourceProvider for eventsBySlices

An alternative to the @apidoc[akka.persistence.query.DurableStateChange] emitted by `changesBySlices` is to store
additional change event when the state is updated or deleted. Those events are stored in the event journal and
can therefore be used with the @ref:[SourceProvider for eventsBySlices](eventsourced.md#sourceprovider-for-eventsbyslices).

Compared to `changesBySlices` the advantages of `eventsBySlices` are:

* can be used with @ref:[Akka Projection gRPC](grpc.md) for asynchronous event based service-to-service communication
* has support for @extref:[Publish events for lower latency](akka-persistence-r2dbc:query.html#publish-events-for-lower-latency-of-eventsbyslices)
* change events can represent smaller deltas than the full state
* all individual change events are received by the consumer, not only the latest as is the case for `changesBySlices`

`DurableState` with change events has the disadvantage that the events must be stored in addition to the latest state. 

You create the change events by defining a @apidoc[ChangeEventHandler] in the `DurableStateBehavior`:

Scala
:  @@snip [DurableStateChangeEventDocExample.scala](/examples/src/test/scala/docs/state/DurableStateChangeEventDocExample.scala) { #changeEventHandler }

Java
:  @@snip [DurableStateChangeEventDocExample.java](/examples/src/test/java/jdocs/state/DurableStateChangeEventDocExample.java) { #changeEventHandler }

In this example we create events that represent deltas of the state changes using the command. In addition to the 
command the `updateHandler` is given the previous state and the new updated state as parameters. If you want to
keep it simple you can use the full state as the change event.

@@@ note

The `updateHandler` and `deleteHandler` are invoked after the ordinary command handler. Be aware of that
if the state is mutable and modified by the command handler the previous state parameter of the `updateHandler`
will also include the modification, since it's the same instance. If that is problem you need to use
immutable state and create a new state instance when modifying it in the command handler.

@@@

The change events are only used for the Projections and is not the source for the `DurableStateBehavior`
state, as they would be with `EventSourcedBehavior`. Therefore, you might not be interested in keeping old events
for too long. Also, the events are not automatically removed when the Durable State is deleted. For event deletion
you can use the @extref:[cleanup tool](akka-persistence-r2dbc:cleanup.html)
