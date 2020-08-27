# Build a Projection handler

While building a projection there are several non-functional requirements to consider.
_What technology to project into? What message delivery semantics are acceptable for the system? Is it compatible with the chosen Source Provider to enable exactly-once message delivery? Does runtime state need to be maintained in the projection while it's running?_
It's up to the user to choose the right answers to these questions, but you must research if the answers to these questions are compatible with each other.

In this guide we will create a Projection that represents shopping cart item popularity.
We will persist our Projection to Cassandra with at-least-once semantics.
The Projection itself will be represented as a Cassandra table.

To proceed we must add the Cassandra Projection library to our project:

@@dependency [sbt,Maven,Gradle] {
group=com.lightbend.akka
artifact=akka-projection-cassandra_$scala.binary.version$
version=$project.version$
}

It's the user's responsibility to implement the means to project into the target system, the Projection itself will only manage the persistence of the offset (though it is possible to enlist your projection into transactions when using projection implementations that support exactly-once like the @ref:[JDBC](../jdbc.md)).
This guide encapsulates its data access layer in a Repository called the `ItemPopularityProjectionRepository`.
The repository will manage a Cassandra table called `item_popularity`.
Each row in `item_popularity` contains a shopping cart item id and a count that represents how often that item was added or removed from all shopping carts.

@@@ note

The example will persist the item popularity count with a [Cassandra counter](https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/counter_type.html) data type.
It's not possible to guarantee that item count updates occur idempotently because we are using at-least-once semantics.
However, since the count is only a rough metric to judge how popular an item is it's not critical to have a totally accurate figure. 

@@@

Add the `ItemPopularityProjectionRepository` to your project:

Scala
:  @@snip [ItemPopularityProjectionRepository.scala](/examples/src/test/scala/docs/guide/ItemPopularityProjectionRepository.scala) { #guideProjectionRepo }

Java
:  @@snip [ItemPopularityProjectionRepository.java](/examples/src/test/java/jdocs/guide/ItemPopularityProjectionRepository.java) { #guideProjectionRepo }

Now it's time to write the Projection handler itself.
This example uses a @apidoc[Handler] that will process `ShoppingCartEvents.Event` events from the @apidoc[SourceProvider] that we implemented earlier.
Specifically, it will only process `ItemEvents` that modify the items added or removed from a shopping cart.
It will ignore all shopping cart `Checkout` events by skipping them.
The event envelopes are processed in the `process` method. 

This example will also log the popularity count of every 10th item event that is processed.
The logging counter is stored as a mutable variable within the handler.
Since this is a simple log operation managing the state in this manner is fine, but to handle more advanced stateful operations you should evaluate using the @apidoc[StatefulHandler].

Scala
:  @@snip [ItemPopularityProjectionHandler.scala](/examples/src/test/scala/docs/guide/ItemPopularityProjectionHandler.scala) { #guideProjectionHandler }

Java
:  @@snip [ItemPopularityProjectionHandler.java](/examples/src/test/java/jdocs/guide/ItemPopularityProjectionHandler.java) { #guideProjectionHandler }

The projection is run by wrapping it in a @apidoc[ProjectionBehavior$] and spawning it as an Actor in the @apidoc[akka.actor.typed.ActorSystem].

Add the following imports to `ShoppingCartApp`:

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionImports }

Java
:  @@snip [ShoppingCartApp.java](/examples/src/test/java/jdocs/guide/ShoppingCartApp.java) { #guideProjectionImports }

Setup the Projection in the Guardian `Behavior` defined in `ShoppingCartApp`:

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionSetup }

Java
:  @@snip [ShoppingCartApp.java](/examples/src/test/java/jdocs/guide/ShoppingCartApp.java) { #guideProjectionSetup }
