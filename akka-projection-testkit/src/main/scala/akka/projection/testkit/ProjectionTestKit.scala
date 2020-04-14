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
@ApiMayChange
object ProjectionTestKit {
  def apply(testKit: ActorTestKit): ProjectionTestKit =
    new ProjectionTestKit(testKit)
}

@ApiMayChange
final class ProjectionTestKit private[akka] (testKit: ActorTestKit) {

  private implicit val system = testKit.system
  private implicit val settings: TestKitSettings = TestKitSettings(system)

  def run(proj: Projection[_])(assertFunc: => Unit): Future[Done] =
    runInternal(proj, assertFunc, settings.SingleExpectDefaultTimeout, 100.millis)

  def run(proj: Projection[_], max: FiniteDuration)(assertFunc: => Unit): Future[Done] = {
    runInternal(proj, assertFunc, max, 100.millis)
  }

  def run(proj: Projection[_], max: FiniteDuration, interval: FiniteDuration)(
      assertFunc: => Unit): Future[Done] = {
    runInternal(proj, assertFunc, max, 100.millis)
  }

  private def runInternal(
      proj: Projection[_],
      assertFunc: => Unit,
      max: FiniteDuration,
      interval: FiniteDuration = 100.millis): Future[Done] = {

    val probe = testKit.createTestProbe[Nothing]("internal-projection-testkit-probe")

    val promiseToStop = Promise[Done]
    try {
      proj.start()
      probe.awaitAssert(assertFunc, max.dilated, interval)
    } finally {
      promiseToStop.completeWith(proj.stop())
    }
    promiseToStop.future
  }
}
