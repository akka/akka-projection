# Changes from Durable State

A typical source for Projections is the change stored with @apidoc[DurableStateBehavior$] in [Akka Persistence](https://doc.akka.io/docs/akka/current/typed/durable-state/persistence.html). Durable state changes can be [tagged](https://doc.akka.io/docs/akka/current/typed/durable-state/persistence.html#tagging) and then
consumed with the [changes query](https://doc.akka.io/docs/akka/current/durable-state/persistence-query.html#using-query-with-akka-projections).

Akka Projections has integration with `changes`, which is described here. 

@@@ note { title=Alternative }
When using the R2DBC plugin an alternative to using a Projection is to @extref:[store the query representation](akka-persistence-r2dbc:durable-state-store.html#storing-query-representation) directly from the write side.
@@@

## Dependencies

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [Maven,sbt,Gradle] {
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
for the `changes` query can be defined with the @apidoc[DurableStateStoreProvider$] like this:

Scala
:  @@snip [DurableStateStoreDocExample.scala](/examples/src/test/scala/docs/state/DurableStateStoreDocExample.scala) { #changesByTagSourceProvider }

Java
:  @@snip [DurableStateStoreDocExample.java](/examples/src/test/java/jdocs/state/DurableStateStoreDocExample.java) { #changesByTagSourceProvider }

This example is using the [DurableStateStore JDBC plugin for Akka Persistence](https://doc.akka.io/docs/akka-persistence-jdbc/current/durable-state-store.html).
You will use the same plugin that you configured for the write side. The one that is used by the `DurableStateBehavior`.

This source is consuming all the changes from the `Account` `DurableStateBehavior` that are tagged with `"bank-accounts-1"`. In a production application, you would need to start as many instances as the number of different tags you used. That way you consume the changes from all entities.

The @scala[`DurableStateChange[AccountEntity.Account]`]@java[`DurableStateChange<AccountEntity.Account>`] is what the `Projection`
handler will process. It contains the `State` and additional meta data, such as the offset that will be stored
by the `Projection`. See @apidoc[akka.persistence.query.DurableStateChange] for full details of what it contains. 

## SourceProvider for changesBySlices

A @apidoc[SourceProvider] defines the source of the envelopes that the `Projection` will process. A `SourceProvider`
for the `changesBySlices` query can be defined with the @apidoc[DurableStateStoreProvider$] like this:

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
