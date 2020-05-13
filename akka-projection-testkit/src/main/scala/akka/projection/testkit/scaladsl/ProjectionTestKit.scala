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
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.ApiMayChange
import akka.projection.Projection
import akka.projection.ProjectionSettings
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink

@ApiMayChange
object ProjectionTestKit {
  def apply(testKit: ActorTestKit): ProjectionTestKit =
    new ProjectionTestKit(testKit)
}

@ApiMayChange
final class ProjectionTestKit private[akka] (testKit: ActorTestKit) {

  private implicit val system = testKit.system
  private implicit val dispatcher = testKit.system.classicSystem.dispatcher
  private implicit val settings: TestKitSettings = TestKitSettings(system)

  def run(projection: Projection[_])(assertFunc: => Unit): Unit =
    runInternal(projection, assertFunc, settings.SingleExpectDefaultTimeout, 100.millis)

  def run(projection: Projection[_], max: FiniteDuration)(assertFunc: => Unit): Unit =
    runInternal(projection, assertFunc, max, 100.millis)

  def run(projection: Projection[_], max: FiniteDuration, interval: FiniteDuration)(assertFunc: => Unit): Unit =
    runInternal(projection, assertFunc, max, interval)

  private def runInternal(
      projection: Projection[_],
      assertFunc: => Unit,
      max: FiniteDuration,
      interval: FiniteDuration): Unit = {

    val probe = testKit.createTestProbe[Nothing]("internal-projection-testkit-probe")

    val settingsForTest = ProjectionSettings(system).withBackoff(0.millis, 0.millis, 0.0, 0)
    val running =
      projection
        .withSettings(settingsForTest)
        .run()(testKit.system.classicSystem)

    try {
      probe.awaitAssert(assertFunc, max.dilated, interval)
    } finally {
      Await.result(running.stop(), max)
    }
  }

  def runWithTestSink[T](projection: Projection[_]): TestSubscriber.Probe[Done] = {
    val sinkProbe = TestSink.probe[Done](testKit.system.toClassic)
    projection.mappedSource().runWith(sinkProbe)
  }

}
