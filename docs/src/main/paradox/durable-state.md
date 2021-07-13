# State changes from Akka Persistence State

A typical source for Projections is changes stored with @apidoc[DurableStateBehavior$] in [Akka Persistence](https://doc.akka.io/docs/akka/current/typed/persistence.html). Durable state changes can be [tagged](https://doc.akka.io/docs/akka/current/typed/persistence.html#tagging) and then
consumed with the [changesByTag query](https://doc.akka.io/docs/akka/current/persistence-query.html#changesbytag-and-currentchangesbytag) <-TODO verify doc ref .

Akka Projections has integration with `changesByTag`, which is described here. 

## Dependencies

To use the Durable State module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-state_$scala.binary.version$
  version=$project.version$
}

Akka Projections require Akka $akka.version$ or later, see @ref:[Akka version](overview.md#akka-version).

@@project-info{ projectId="akka-projection-state" }

### Transitive dependencies

The table below shows `akka-projection-state`'s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-state" }

## SourceProvider for changesByTag

A @apidoc[SourceProvider] defines the source of the event envelopes that the `Projection` will process. A `SourceProvider`
for the `changesByTag` query can be defined with the @apidoc[DurableStateStoreProvider$] like this:

Scala
:  @@snip [DurableStateStoreDocExample.scala](/examples/src/test/scala/docs/state/DurableStateStoreDocExample.scala) { #imports #sourceProvider }

Java
:  @@snip [DurableStateStoreDocExample.java](/examples/src/test/java/jdocs/state/DurableStateStoreDocExample.java) { #imports #sourceProvider }

TODO verify link to jdbc state store
This example is using the [DurableStateStore JDBC plugin for Akka Persistence](https://doc.akka.io/docs/akka-persistence/current/jdbc-state-store.html).
You will use the same plugin as you have configured for the write side that is used by the `DurableStateBehavior`.

This source is consuming all changes from the `Rewards` `DurableStateBehavior` that are tagged with `"user-1"`.

The tags are assigned as described in @ref:[Tagging Changes in DurableStateBehavior](running.md#tagging-changes-in-durablestatebehavior).

The @scala[`DurableStateChange[Rewards.State]`]@java[`DurableStateChange<Rewards.State>`] is what the `Projection`
handler will process. It contains the `State` and additional meta data, such as the offset that will be stored
by the `Projection`. See @apidoc[akka.projection.state.DurableStateChange] for full details of what it contains. 
