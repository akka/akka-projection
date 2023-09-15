# Testing

Akka Projections provides a TestKit to ease testing. There are two supported styles of test: running with an assert function and driving it with an Akka Streams TestKit `TestSink` probe.

## Dependencies

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

To use the Akka Projections TestKit add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-testkit_$scala.binary.version$
  version=$project.version$
  scope="test"
}

Akka Projections require Akka $akka.version$ or later, see @ref:[Akka version](overview.md#akka-version).

@@project-info{ projectId="akka-projection-testkit" }

### Transitive dependencies

The table below shows `akka-projection-testkit`'s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-testkit" }

## Initializing the Projection TestKit

The Projection TestKit requires an instance of `ActorTestKit`. We recommend using Akka's @scala[`ScalaTestWithActorTestKit`]@java[`TestKitJunitResource`]

Scala
:  @@snip [TestKitDocExample.scala](/examples/src/test/scala/docs/testkit/TestKitDocExample.scala) { #testkit-import #testkit }

Java
:  @@snip [TestKitDocExample.java](/examples/src/test/java/jdocs/testkit/TestKitDocExample.java) { #testkit-import #testkit }

## Testing with an assert function

When testing with an assert function the Projection is started and stopped by the TestKit. While the projection is running, the assert function will be called until it completes without errors (no exceptions or assertion errors are thrown).

In the example below the Projection will update a `CartView`. The test will run until it observes that the `CartView` for id `abc-def` is available in the repository.  

Scala
:  @@snip [TestKitDocExample.scala](/examples/src/test/scala/docs/testkit/TestKitDocExample.scala) { #testkit-import #testkit-run }

Java
:  @@snip [TestKitDocExample.java](/examples/src/test/java/jdocs/testkit/TestKitDocExample.java) { #testkit-run }

By default, the test will run for 3 seconds. The assert function will be called every 100 milliseconds. Those values can be modified programatically.


Note: when testing a Projection with this method, the Restart Backoff is disabled. Any backoff configuration settings from `.conf` file or programmatically added will be overwritten.

Scala
:  @@snip [TestKitDocExample.scala](/examples/src/test/scala/docs/testkit/TestKitDocExample.scala) { #testkit-duration #testkit-run-max-interval }

Java
:  @@snip [TestKitDocExample.java](/examples/src/test/java/jdocs/testkit/TestKitDocExample.java) { #testkit-duration #testkit-run-max-interval }  

## Testing with a TestSink probe

The [Akka Stream TestKit](https://doc.akka.io/docs/akka/current/stream/stream-testkit.html#using-the-testkit) can be used to drive the pace of envelopes flowing through the Projection.

The Projection starts as soon as the first element is requested by the `TestSink` probe, new elements will be emitted as requested. The Projection is stopped once the assert function completes.

Scala
:  @@snip [TestKitDocExample.scala](/examples/src/test/scala/docs/testkit/TestKitDocExample.scala) { #testkit-sink-probe }

Java
:  @@snip [TestKitDocExample.java](/examples/src/test/java/jdocs/testkit/TestKitDocExample.java) { #testkit-assertion-import #testkit-sink-probe }

## Testing with mocked Projection and SourceProvider

To test a handler in isolation you may want to mock out the implementation of a Projection or SourceProvider so that you don't have to setup and teardown the associated technology as part of your _integration_ test.
For example, you may want to project against a Cassandra database, or read envelopes from an Akka Persistence journal source, but you don't want to have to run Docker containers or embedded/in-memory services just to run your tests.
The @apidoc[TestProjection] allows you to isolate the runtime of your handler so that you don't need to run these services.
Using a `TestProjection` has the added benefit of being fast, since you can run everything within the JVM that runs your tests.

Alongside the `TestProjection` is the @apidoc[TestSourceProvider] which can be used to provide test data to the `TestProjection` running the handler.
Test data can be represented in an akka streams @apidoc[akka.stream.(javadsl|scaladsl).Source] that is passed to the `TestSourceProvider` constructor.

Scala
:  @@snip [TestKitDocExample.scala](/examples/src/test/scala/docs/testkit/TestKitDocExample.scala) { #testkit-testprojection }

Java
:  @@snip [TestKitDocExample.scala](/examples/src/test/java/jdocs/testkit/TestKitDocExample.java) { #testkit-testprojection }
