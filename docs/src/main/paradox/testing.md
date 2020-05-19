# Testing

Akka Projection provides a TestKit to ease testing. There are two supported styles of test: running with an assert function and driving it with an Akka Streams TestKit `TestSink`.

## Dependencies

To use the Akka Projection TestKit add the following dependency in your project:

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

Scala
:  @@snip [TestKitDocExample.scala](/examples/src/test/scala/docs/testkit/TestKitDocExample.scala) { #testkit-duration #testkit-run-max-interval }

Java
:  @@snip [TestKitDocExample.java](/examples/src/test/java/jdocs/testkit/TestKitDocExample.java) { #testkit-duration #testkit-run-max-interval }  

## Testing with a TestSink

The [Akka Stream TestKit](https://doc.akka.io/docs/akka/current/stream/stream-testkit.html#using-the-testkit) can be used to drive the pace of envelopes flowing through the Projection.

 The Projection starts as soon as the first element is requested by the `TestSink`, new elements will be emitted as requested by the `TestSink`. The Projection won't stop by itself, therefore it's recommended to cancel the `TestSink` probe to gracefully stop the Projection.

Scala
:  @@snip [TestKitDocExample.scala](/examples/src/test/scala/docs/testkit/TestKitDocExample.scala) { #testkit-sink-probe }

Java
:  @@snip [TestKitDocExample.java](/examples/src/test/java/jdocs/testkit/TestKitDocExample.java) { #testkit-assertion-import #testkit-sink-probe }
