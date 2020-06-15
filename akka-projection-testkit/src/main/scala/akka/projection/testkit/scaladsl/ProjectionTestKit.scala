/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.ApiMayChange
import akka.projection.Projection
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.RestartBackoffSettings
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink

@ApiMayChange
object ProjectionTestKit {
  def apply(testKit: ActorTestKit): ProjectionTestKit =
    new ProjectionTestKit(testKit)
}

@ApiMayChange
final class ProjectionTestKit private[akka] (testKit: ActorTestKit) {

  private implicit val system: ActorSystem[Nothing] = testKit.system
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
   * @param assertFunction - a by-name code block that exercise the test assertions
   */
  def run(projection: Projection[_])(assertFunction: => Unit): Unit =
    runInternal(projection, assertFunction, settings.SingleExpectDefaultTimeout, 100.millis)

  /**
   * Run a Projection and assert its projected data using the passed assert function and the max duration of the test.
   *
   * Projection is started and stopped by the TestKit. While the projection is running, the assert function
   * will be called every 100 milliseconds until it completes without errors (no exceptions or assertion errors are thrown).
   *
   * If the assert function doesn't complete without error within the passed `max` duration the test will fail.
   *
   * @param projection - the Projection to run
   * @param max - FiniteDuration delimiting the max duration of the test
   * @param assertFunction - a by-name code block that exercise the test assertions
   */
  def run(projection: Projection[_], max: FiniteDuration)(assertFunction: => Unit): Unit =
    runInternal(projection, assertFunction, max, 100.millis)

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
   * @param max - FiniteDuration delimiting the max duration of the test
   * @param interval - FiniteDuration defining the internval in each the assert function will be called
   * @param assertFunction - a by-name code block that exercise the test assertions
   */
  def run(projection: Projection[_], max: FiniteDuration, interval: FiniteDuration)(assertFunction: => Unit): Unit =
    runInternal(projection, assertFunction, max, interval)

  private def runInternal(
      projection: Projection[_],
      assertFunction: => Unit,
      max: FiniteDuration,
      interval: FiniteDuration): Unit = {

    val probe = testKit.createTestProbe[Nothing]("internal-projection-testkit-probe")

    val actorHandler = spawnActorHandler(projection)

    val settingsForTest = ProjectionSettings(system).copy(RestartBackoffSettings(0.millis, 0.millis, 0.0, 0))
    val running =
      projection
        .withSettings(settingsForTest)
        .run()(testKit.system)
    try {
      probe.awaitAssert(assertFunction, max.dilated, interval)
    } finally {
      Await.result(running.stop(), max)
      actorHandler.foreach(ref => testKit.stop(ref))
    }
  }

  /**
   * Run a Projection with an attached `TestSubscriber.Probe` allowing
   * control over the pace in which the elements flow through the Projection.
   *
   * The assertion function receives a `TestSubscriber.Probe` that you can use
   * request elements.
   *
   * The Projection starts as soon as the first element is requested by the `TestSubscriber.Probe`, new elements will be emitted
   * as requested. The Projection is stopped once the assert function completes.
   *
   * @param projection - the Projection to run
   * @param assertFunction - a function receiving a `TestSubscriber.Probe[Done]`
   */
  def runWithTestSink(projection: Projection[_])(assertFunction: TestSubscriber.Probe[Done] => Unit): Unit = {
    val actorHandler = spawnActorHandler(projection)
    val sinkProbe = projection.mappedSource().runWith(TestSink.probe[Done](testKit.system.classicSystem))
    try {
      assertFunction(sinkProbe)
    } finally {
      sinkProbe.cancel()
      actorHandler.foreach(ref => testKit.stop(ref))
    }
  }

  private def spawnActorHandler(projection: Projection[_]): Option[ActorRef[_]] = {
    projection.actorHandlerInit[Any].map { init =>
      val ref = testKit.spawn(Behaviors.supervise(init.behavior).onFailure(SupervisorStrategy.restart))
      init.setActor(ref)
      ref
    }
  }

}
