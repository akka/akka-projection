# Writing tests for a Projection

Like other akka libraries, Projections ships with a @ref:[TestKit](../testing.md) that a user can include to assert the correctness of their Projection handler implementation.
Add the Projections TestKit dependency to your project:

@@dependency [sbt,Maven,Gradle] {
group=com.lightbend.akka
artifact=akka-projection-testkit_$scala.binary.version$
version=$project.version$
}

Import the @apidoc[akka.projection.testkit.(javadsl|scaladsl).ProjectionTestKit] and other utilities into a new [ScalaTest](https://www.scalatest.org/) test spec.

Scala
:  @@snip [ShoppingCartAppSpec.scala](/examples/src/test/scala/docs/guide/ShoppingCartAppSpec.scala) { #testKitImports }

The TestKit includes several utilities to run the Projection handler in isolation so that a full projection implementation and source provider are not required.

* @apidoc[akka.projection.testkit.(javadsl|scaladsl).ProjectionTestKit] takes an Akka @apidoc[akka.actor.testkit.typed.(javadsl|scaladsl).ActorTestKit] and runs a projection with the test @apidoc[akka.actor.typed.ActorSystem].
* @apidoc[TestSourceProvider] allows the user to mock out test data `Envelopes` that will be processed by the Projection Handler.
* @apidoc[TestProjection] is a test Projection implementation that uses an in-memory internal offset store.

Using these tools we can assert that our Projection handler meets the following requirements of the `CheckoutProjectionHandler`.

1. Write checkout events to the database each time they're encountered.
1. Log the last 10 checked out carts.

Scala
:  @@snip [ShoppingCartAppSpec.scala](/examples/src/test/scala/docs/guide/ShoppingCartAppSpec.scala) { #testKitSpec }

To run the tests from the command line run the following sbt command.

```
sbt "examples/testOnly docs.guide.ShoppingCartAppSpec"
```