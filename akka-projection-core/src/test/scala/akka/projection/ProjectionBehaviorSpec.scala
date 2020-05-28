/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorSystem
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike

object ProjectionBehaviorSpec {

  sealed trait ProbeMessage
  case object StartObserved extends ProbeMessage
  case class Consumed(n: Int, currentState: String) extends ProbeMessage
  case object StopObserved extends ProbeMessage

  /*
   * This TestProjection has a internal state that we can use to prove that on restart,
   * the actor is taking a new projection instance.
   */
  case class TestProjection(src: Source[Int, NotUsed], testProbe: TestProbe[ProbeMessage], failToStop: Boolean = false)
      extends Projection[Int] {

    override def projectionId: ProjectionId = ProjectionId("test-projection-with-internal-state", "00")

    override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection =
      new InternalProjectionState(testProbe, failToStop).newRunningInstance()

    override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, _] =
      new InternalProjectionState(testProbe, failToStop).mappedSource()

    override def withSettings(settings: ProjectionSettings): Projection[Int] =
      this // no need for ProjectionSettings in tests

    /*
     * INTERNAL API
     * This internal class will hold the KillSwitch that is needed
     * when building the mappedSource and when running the projection (to stop)
     */
    private class InternalProjectionState(testProbe: TestProbe[ProbeMessage], failToStop: Boolean = false)(
        implicit val system: ActorSystem[_]) {

      private val strBuffer = new StringBuffer("")

      private val killSwitch = KillSwitches.shared(projectionId.id)

      def mappedSource(): Source[Done, _] =
        src.via(killSwitch.flow).mapAsync(1)(i => process(i))

      private def process(i: Int): Future[Done] = {

        if (strBuffer.toString.isEmpty) strBuffer.append(i)
        else strBuffer.append("-").append(i)

        testProbe.ref ! Consumed(i, strBuffer.toString)
        Future.successful(Done)
      }

      def newRunningInstance(): RunningProjection =
        new TestRunningProjection(mappedSource(), testProbe, failToStop, killSwitch)
    }

    private class TestRunningProjection(
        source: Source[Done, _],
        testProbe: TestProbe[ProbeMessage],
        failToStop: Boolean = false,
        killSwitch: SharedKillSwitch)(implicit system: ActorSystem[_])
        extends RunningProjection {

      testProbe.ref ! StartObserved
      private val futureDone = source.run()

      override def stop()(implicit ec: ExecutionContext): Future[Done] = {
        val stopFut =
          if (failToStop) {
            // this simulates a failure when stopping the stream
            // for the ProjectionBehavior the effect is the same
            Future.failed(new RuntimeException("failed to stop properly"))
          } else {
            killSwitch.shutdown()
            futureDone
          }
        stopFut.onComplete(_ => testProbe.ref ! StopObserved)
        stopFut
      }
    }
  }
}
class ProjectionBehaviorSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  import ProjectionBehaviorSpec._

  "A ProjectionBehavior" must {

    "start immediately on demand" in {

      val testProbe = testKit.createTestProbe[ProbeMessage]()
      val src = Source(1 to 2)
      testKit.spawn(ProjectionBehavior(TestProjection(src, testProbe)))

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))
      // good, things are flowing

    }

    "stop after receiving stop message" in {

      val testProbe = testKit.createTestProbe[ProbeMessage]()
      val src = Source(1 to 2)
      val projectionRef = testKit.spawn(ProjectionBehavior(TestProjection(src, testProbe)))

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))
      // good, things are flowing

      projectionRef ! ProjectionBehavior.Stop
      testProbe.expectMessage(StopObserved)

      testProbe.expectTerminated(projectionRef)

    }

    "also stop when stopping underlying stream results in failure" in {

      val testProbe = testKit.createTestProbe[ProbeMessage]()
      val src = Source(1 to 2)
      val projectionRef = testKit.spawn(ProjectionBehavior(TestProjection(src, testProbe, failToStop = true)))

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))
      // good, things are flowing

      // this Stop will crash the actor
      projectionRef ! ProjectionBehavior.Stop
      testProbe.expectMessage(StopObserved)

      testProbe.expectTerminated(projectionRef)

    }
  }

}
