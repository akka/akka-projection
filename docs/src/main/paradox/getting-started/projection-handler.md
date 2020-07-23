# Build a Projection handler

While building a projection there are several non-functional requirements to consider.
_What technology to project into? What message delivery semantics are acceptable for the system? Is it compatible with the chosen Source Provider to enable exactly-once message delivery? Does runtime state need to be maintained in the projection while it's running?_
It's up to the user to choose the right answers to these questions, but you must research if the answers to these questions compatible with each other.

For this guide we will create a Projection that represents cart checkout state.
We will persist our Projection to Cassandra with at-least-once semantics.
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
This guide encapsulates its data access layer in a Repository called the `CheckoutProjectionRepository`.
The repository will manage a Cassandra table called `cart_checkout_state`.

Each row in `cart_checkout_state` contains a shopping cart id, a timestamp for when the cart was last updated, and optionally a timestamp for when the cart was checked out.
When the shopping cart is modified or checked out we update the last updated timestamp.
When the shopping cart is checked out we update last updated and checkout timestamp.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionRepo }

Now it's time to write the Projection handler itself.
This example uses a @apidoc[Handler] that will process `ShoppingCartEvents.Event` events from the @apidoc[SourceProvider] that we implemented earlier.
The event envelopes are processed in the `process` method. 
Each type of event results in a different action to take against the Projection tables.

This example will also log the last 10 checked out carts and their contents every 10 checkout events that are processed.
The count of checkout events that are processed is represented with a mutable variable within the handler.
Because it's not critical that the logging counter be accurate from the start of the projection we can keep it in a simple local volatile variable.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionHandler }

The projection is run by wrapping it in a @apidoc[ProjectionBehavior] and running it with a typed @apidoc[akka.actor.typed.ActorSystem].

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionSetup }
