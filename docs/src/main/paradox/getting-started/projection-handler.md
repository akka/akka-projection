# Build a Stateful Projection handler

While building a projection there are several non-functional requirements to consider.
_What technology to project into? What message delivery semantics are acceptable for the system? Is it compatible with the chosen Source Provider to enable exactly-once message delivery? Does runtime state need to be maintained in the projection while it's running?_
It's up to the user to choose the right answers to these questions, but you must research if the answers to these questions compatible with each other.

For this guide we will create a Projection that represents all checked out shopping carts and all the items that were purchased.
For the purpose of this guide we will persist our Projection to Cassandra with at-least-once semantics.
The Projection itself will be represented as a Cassandra table.

To proceed we must add the Cassandra Projection library to our project:

@@dependency [sbt,Maven,Gradle] {
group=com.lightbend.akka
artifact=akka-projection-cassandra_$scala.binary.version$
version=$project.version$
}

Add the following imports to your project:

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionImports }

It's the user's responsibility to implement the means to project into the target system, the Projection itself will only manage the persistence of the offset (though it is possible to enlist your projection into transactions when using projection implementations that support exactly-once like the @ref:[JDBC](../jdbc.md)).
This guide encapsulates its data access layer in a Repository called the `DailyCheckoutProjectionRepository`.
The repository will manage two Cassandra tables:

1. The `daily_checkouts` table will contain the actual Projection data of checked out shopping carts.
Each row in the table will include a date, shopping cart id, item id, and item quantity.
End users will be able to query the table by a date, cart id, or item id.
2. The `cart_state` table is an intermediate table that represents the current state of all active shopping carts.
This is required so that we can restore our state after the projection has been shutdown (i.e. a previously planned shutdown, a failure, or a rebalance).
This table can also be considered a Projection, even though we are only using it to represent intermediate state in this guide.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionRepo }

Now it's time to write the Projection handler itself.
This example uses a @apidoc[StatefulHandler] that will process `ShoppingCartEvents.Event` events from the @apidoc[SourceProvider] that we implemented earlier.
Since this is a @apidoc[StatefulHandler] we will load our initial shopping cart state into memory by implementing `initialState`.
The event envelopes are processed in the `process` method. 
Each type of event results in a different action to take against the Projection tables.

This example will also occasionally log all the checked out carts and their contents every 10 checkout events that are processed.
The count of checkout events that are processed is represented with a mutable variable within the handler.
Because it's not critical that the logging counter be accurate from the start of the projection we can keep it in a simple local volatile variable.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionHandler }

The projection is run by wrapping it in a @apidoc[ProjectionBehavior] and running it with a typed @apidoc[akka.actor.typed.ActorSystem].

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionSetup }
