#  Choosing a Source Provider

A @apidoc[SourceProvider] will provide the data to our projection. 
In Projections each element that's processed is an `Envelope` and each `Envelope` contains an `Event`.
An `Envelope` must include an `Offset`, but it can also contain other information such as creation timestamp, a topic name, an entity tag, etc.
There are several supported Source Provider's available (or you can build your own), but in this example we will use the @ref:[Akka Persistence `EventSourced` Source Provider](../eventsourced.md).

Add the following dependencies to your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-eventsourced_$scala.binary.version$
  version=$project.version$
}

Add the following imports to `ShoppingCartApp`:

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideSourceProviderImports }

Java
:  @@snip [ShoppingCartApp.java](/examples/src/test/java/jdocs/guide/ShoppingCartApp.java) { #guideSourceProviderImports }

Create the @apidoc[SourceProvider].
The @ref:[Event Sourced Source Provider](../eventsourced.md) is using [Akka Persistence](https://doc.akka.io/docs/akka/current/typed/persistence.html) internally (specifically the [eventsByTag](https://doc.akka.io/docs/akka/current/persistence-query.html#eventsbytag-and-currenteventsbytag) API).
To initialize the Source Provider we need to set parameters to choose the Akka Persistence plugin (Cassandra) to use as well as the name of the tag used for events we're interested in from the journal.

Setup the `SourceProvider` in the Guardian `Behavior` defined in `ShoppingCartApp`:

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideSourceProviderSetup }

Java
:  @@snip [ShoppingCartApp.java](/examples/src/test/java/jdocs/guide/ShoppingCartApp.java) { #guideSourceProviderSetup }

Finally, we must configure Akka Persistence by adding a configuration file `guide-shopping-cart-app.conf` to the `src/main/resources/` directory of the project:

@@snip [guide-shopping-cart-app.conf](/examples/src/test/resources/guide-shopping-cart-app.conf) { #guideConfig }
