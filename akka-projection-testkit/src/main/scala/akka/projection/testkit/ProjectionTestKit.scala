/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.Done
import akka.actor.ActorSystem
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, _ }
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.ApiMayChange
import akka.projection.Projection
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink

import scala.concurrent.Await
import scala.concurrent.duration._

@ApiMayChange
object ProjectionTestKit {
  def apply(testKit: ActorTestKit): ProjectionTestKit =
    new ProjectionTestKit(testKit)
}

@ApiMayChange
final class ProjectionTestKit private[akka] (testKit: ActorTestKit) {

  private implicit val settings: TestKitSettings = TestKitSettings(testKit.system)

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

    import testKit.system

    val probe = testKit.createTestProbe[Nothing]("internal-projection-testkit-probe")
    try {
      projection.run()
      probe.awaitAssert(assertFunc, max.dilated, interval)
    } finally {
      Await.result(projection.stop(), max)
    }
  }

  def runWithTestSink[T](projection: Projection[_]): TestSubscriber.Probe[Done] = {
    implicit val classicSys: ActorSystem = testKit.system.toClassic
    val sinkProbe = TestSink.probe[Done]
    projection.mappedSource.runWith(sinkProbe)
  }

}
