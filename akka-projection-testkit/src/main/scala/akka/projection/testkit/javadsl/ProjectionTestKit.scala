/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.javadsl

import java.time.Duration

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.javadsl.ActorTestKit
import akka.actor.typed.javadsl.Adapter
import akka.annotation.ApiMayChange
import akka.projection.Projection
import akka.projection.ProjectionSettings
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.javadsl.TestSink
import akka.util.JavaDurationConverters._

@ApiMayChange
object ProjectionTestKit {
  def create(testKit: ActorTestKit): ProjectionTestKit =
    new ProjectionTestKit(testKit)
}

@ApiMayChange
final class ProjectionTestKit private[akka] (testKit: ActorTestKit) {

  private implicit val system = testKit.system
  private implicit val dispatcher = testKit.system.classicSystem.dispatcher
  private implicit val settings: TestKitSettings = TestKitSettings(system)

  /**
   * Run a Projection and assert its projected data using the passed assert function.
   *
   * Projection is started and stopped by the TestKit. While the projection is running, the assert function
   * will be called every 100 milliseconds until it completes without errors (no exceptions or assertion errors are thrown).
   *
   * If the assert function doesn't complete without error within 3 seconds the test will fail.
   *
   * @param projection - the Projection to run
   * @param assertFunction - a Runnable that exercise the test assertions
   */
  def run(projection: Projection[_], assertFunction: Runnable): Unit =
    runInternal(projection, assertFunction, settings.SingleExpectDefaultTimeout.asJava, 100.millis.asJava)

  /**
   * Run a Projection and assert its projected data using the passed assert function and the max duration of the test.
   *
   * Projection is started and stopped by the TestKit. While the projection is running, the assert function
   * will be called every 100 milliseconds until it completes without errors (no exceptions or assertion errors are thrown).
   *
   * If the assert function doesn't complete without error within the passed `max` duration the test will fail.
   *
   * @param projection - the Projection to run
   * @param max - Duration delimiting the max duration of the test
   * @param assertFunction - a Runnable that exercise the test assertions
   */
  def run(projection: Projection[_], max: Duration, assertFunction: Runnable): Unit =
    runInternal(projection, assertFunction, max, 100.millis.asJava)

  /**
   * Run a Projection and assert its projected data using the passed assert function,
   * the max duration of the test and the interval between each assertion.
   *
   * Projection is started and stopped by the TestKit. While the projection is running, the assert function
   * will be called every `interval` until it completes without errors (no exceptions or assertion errors are thrown).
   *
   * If the assert function doesn't complete without error within the passed `max` duration the test will fail.
   *
   * @param projection - the Projection to run
   * @param max - Duration delimiting the max duration of the test
   * @param interval - Duration defining the internval in each the assert function will be called
   * @param assertFunction - a Runnable that exercise the test assertions
   */
  def run(projection: Projection[_], max: Duration, interval: Duration, assertFunction: Runnable): Unit =
    runInternal(projection, assertFunction, max, interval)

  private def runInternal(
      projection: Projection[_],
      assertFunction: Runnable,
      max: Duration,
      interval: Duration): Unit = {

    val probe = testKit.createTestProbe[Nothing]("internal-projection-testkit-probe")

    val settingsForTest = ProjectionSettings(system).withBackoff(0.millis, 0.millis, 0.0, 0)

    val running =
      projection
        .withSettings(settingsForTest)
        .run()(testKit.system.classicSystem)

    try {
      probe.awaitAssert(max, interval, () => {
        assertFunction.run()
        Done
      })
    } finally {
      Await.result(running.stop(), max.asScala)
    }
  }

  /**
   * Run a Projection with an attached `TestSink` allowing
   * control over the pace the elements flow through the Projection.
   *
   * The Projection starts as soon as the first element is requested by the `TestSink`, new elements will be emitted
   * as requested by the `TestSink`. The Projection won't stop by itself, therefore it's recommended to cancel the
   * `TestSink` probe to gracefully stop the Projection.
   *
   * @param projection - the Projection to run
   */
  def runWithTestSink[T](projection: Projection[_]): TestSubscriber.Probe[Done] = {
    val sinkProbe = TestSink.probe[Done](Adapter.toClassic(testKit.system))
    projection.mappedSource().runWith(sinkProbe)
  }

}
