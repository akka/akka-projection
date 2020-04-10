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

  def run(proj: Projection[_])(testFunc: => Unit): Future[Done] =
    runInternal(proj, testFunc, settings.SingleExpectDefaultTimeout.dilated, 100.millis)

  def run(proj: Projection[_], max: FiniteDuration)(testFunc: => Unit): Future[Done] = {
    runInternal(proj, testFunc, max, 100.millis)
  }

  def run(proj: Projection[_], max: FiniteDuration, interval: FiniteDuration = 100.millis)(
      testFunc: => Unit): Future[Done] = {
    runInternal(proj, testFunc, max, 100.millis)
  }

  private def runInternal(
      proj: Projection[_],
      testFunc: => Unit,
      max: FiniteDuration,
      interval: FiniteDuration = 100.millis): Future[Done] = {

    val probe = testKit.createTestProbe[Nothing]("internal-projection-testkit-probe")

    val promiseToStop = Promise[Done]
    try {
      proj.start()
      probe.awaitAssert(testFunc, max, interval)
    } finally {
      promiseToStop.completeWith(proj.stop())
    }
    promiseToStop.future
  }
}
