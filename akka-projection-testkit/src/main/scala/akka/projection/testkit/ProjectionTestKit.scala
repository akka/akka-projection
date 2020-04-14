/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.Done
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, _ }
import akka.annotation.ApiMayChange
import akka.projection.Projection

import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }
import scala.concurrent.Await
@ApiMayChange
object ProjectionTestKit {
  def apply(testKit: ActorTestKit): ProjectionTestKit =
    new ProjectionTestKit(testKit)
}

@ApiMayChange
final class ProjectionTestKit private[akka] (testKit: ActorTestKit) {

  private implicit val system = testKit.system
  private implicit val settings: TestKitSettings = TestKitSettings(system)

  def run(proj: Projection[_])(assertFunc: => Unit): Unit =
    runInternal(proj, assertFunc, settings.SingleExpectDefaultTimeout, 100.millis)

  def run(proj: Projection[_], max: FiniteDuration)(assertFunc: => Unit): Unit =
    runInternal(proj, assertFunc, max, 100.millis)

  def run(proj: Projection[_], max: FiniteDuration, interval: FiniteDuration)(assertFunc: => Unit): Unit =
    runInternal(proj, assertFunc, max, 100.millis)

  private def runInternal(
      proj: Projection[_],
      assertFunc: => Unit,
      max: FiniteDuration,
      interval: FiniteDuration = 100.millis): Unit = {

    val probe = testKit.createTestProbe[Nothing]("internal-projection-testkit-probe")

    try {
      proj.start()
      probe.awaitAssert(assertFunc, max.dilated, interval)
    } finally {
      Await.ready(proj.stop(), max)
    }
  }
}
