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

  def run(projection: Projection[_], runnable: Runnable): Unit =
    runInternal(projection, runnable, settings.SingleExpectDefaultTimeout.asJava, 100.millis.asJava)

  def run(projection: Projection[_], max: Duration, runnable: Runnable): Unit =
    runInternal(projection, runnable, max, 100.millis.asJava)

  def run(projection: Projection[_], max: Duration, interval: Duration, runnable: Runnable): Unit =
    runInternal(projection, runnable, max, interval)

  private def runInternal(projection: Projection[_], runnable: Runnable, max: Duration, interval: Duration): Unit = {

    val probe = testKit.createTestProbe[Nothing]("internal-projection-testkit-probe")

    val settingsForTest = ProjectionSettings(system).withBackoff(0.millis, 0.millis, 0.0, 0)

    val running =
      projection
        .withSettings(settingsForTest)
        .run()(testKit.system.classicSystem)

    try {
      probe.awaitAssert(max, interval, () => {
        runnable.run()
        Done
      })
    } finally {
      Await.result(running.stop(), max.asScala)
    }
  }

  def runWithTestSink[T](projection: Projection[_]): TestSubscriber.Probe[Done] = {
    val sinkProbe = TestSink.probe[Done](Adapter.toClassic(testKit.system))
    projection.mappedSource().runWith(sinkProbe)
  }

}
