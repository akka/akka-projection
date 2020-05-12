/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

import akka.Done
import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
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

    private val strBuffer = new StringBuffer("")
    override def projectionId: ProjectionId = ProjectionId("test-projection-with-internal-state", "00")

    override def run()(implicit systemProvider: ClassicActorSystemProvider): RunningProjection =
      new TestProjectionState().newRunningInstance()

    private def process(i: Int): Future[Done] = {
      concat(i)
      testProbe.ref ! Consumed(i, strBuffer.toString)
      Future.successful(Done)
    }

    private[projection] def mappedSource()(implicit systemProvider: ClassicActorSystemProvider): Source[Done, _] =
      new TestProjectionState().mappedSource()

    private def concat(i: Int) = {
      if (strBuffer.toString.isEmpty) strBuffer.append(i)
      else strBuffer.append("-").append(i)
    }

    override def withSettings(settings: ProjectionSettings): Projection[Int] =
      this // no need for ProjectionSettings in tests

    private class TestProjectionState(implicit val systemProvider: ClassicActorSystemProvider) {

      private val killSwitch = KillSwitches.shared(projectionId.id)

      def mappedSource(): Source[Done, _] =
        src.via(killSwitch.flow).mapAsync(1)(i => process(i))

      def newRunningInstance(): RunningProjection =
        new TestRunningProjection(mappedSource(), killSwitch)
    }

    private class TestRunningProjection(val source: Source[Done, _], killSwitch: SharedKillSwitch)(
        implicit val systemProvider: ClassicActorSystemProvider)
        extends RunningProjection {

      private val promiseToStop: Promise[Done] = Promise()

      testProbe.ref ! StartObserved
      val done = source.run()
      promiseToStop.completeWith(done)

      override def stop()(implicit ec: ExecutionContext): Future[Done] = {
        val stopFut =
          if (failToStop) {
            // this simulates a failure when stopping the stream
            // for the ProjectionBehavior the effect is the same
            Future.failed(new RuntimeException("failed to stop properly"))
          } else {
            killSwitch.shutdown()
            promiseToStop.future
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
