# Writing tests for a Projection

Like other Akka libraries, Projections ships with a @ref:[TestKit](../testing.md) that a user can include to assert the correctness of their Projection handler implementation.
Add the Projections TestKit dependency to your project:

@@dependency [sbt,Maven,Gradle] {
group=com.lightbend.akka
artifact=akka-projection-testkit_$scala.binary.version$
version=$project.version$
}

Import the @apidoc[akka.projection.testkit.(javadsl|scaladsl).ProjectionTestKit] and other utilities into a new 
@scala[[ScalaTest](https://doc.akka.io/docs/akka/current/typed/testing-async.html#test-framework-integration) test spec]
@java[[JUnit](https://doc.akka.io/docs/akka/current/typed/testing-async.html#test-framework-integration) test].

Scala
:  @@snip [ShoppingCartAppSpec.scala](/examples/src/test/scala/docs/guide/ShoppingCartAppSpec.scala) { #testKitImports }

The TestKit includes several utilities to run the Projection handler in isolation so that a full projection implementation and source provider are not required.

* @apidoc[akka.projection.testkit.(javadsl|scaladsl).ProjectionTestKit] runs a projection with the test @apidoc[akka.actor.typed.ActorSystem].
* @apidoc[TestSourceProvider] allows the user to mock out test data `Envelopes` that will be processed by the Projection Handler.
* @apidoc[TestProjection] is a test Projection implementation that uses an in-memory internal offset store.

Using these tools we can assert that our Projection handler meets the following requirements of the `ItemPopularityProjectionHandler`.

1. Process each shopping cart item event, correctly calculate the item count delta, and update the database.
1. Log the popularity of every 10th shopping cart item event that is processed.

Scala
:  @@snip [ShoppingCartAppSpec.scala](/examples/src/test/scala/docs/guide/ShoppingCartAppSpec.scala) { #testKitSpec }

To run the tests from the command line run the following sbt command.

```
sbt "examples/testOnly docs.guide.ShoppingCartAppSpec"
```
