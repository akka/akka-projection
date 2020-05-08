/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

import akka.Done
import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.stream.KillSwitches
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike

class ProjectionBehaviorSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "A ProjectionBehavior" must {

    "start immediately on demand" in {

      val testProbe = testKit.createTestProbe[ProbeMessage]()
      val src = Source(1 to 2)
      testKit.spawn(ProjectionBehavior(() => TestProjection(src, testProbe)))

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))
      // good, things are flowing

    }

    "stop actor when receiving stop message" in {

      val testProbe = testKit.createTestProbe[ProbeMessage]()
      val src = Source(1 to 2)
      val projectionRef = testKit.spawn(ProjectionBehavior(() => TestProjection(src, testProbe)))

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))
      // good, things are flowing

      projectionRef ! ProjectionBehavior.Stop
      testProbe.expectMessage(StopObserved)

      testProbe.expectTerminated(projectionRef)

    }

    "failures stopping stream don't block stopping the actor" in {

      val testProbe = testKit.createTestProbe[ProbeMessage]()
      val src = Source(1 to 2)
      val projectionRef = testKit.spawn(ProjectionBehavior(() => TestProjection(src, testProbe, failToStop = true)))

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))
      // good, things are flowing

      // this Stop will crash the actor
      projectionRef ! ProjectionBehavior.Stop

      testProbe.expectTerminated(projectionRef)

    }
  }

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

    private val killSwitch = KillSwitches.shared(projectionId.id)
    private val promiseToStop = Promise[Done]

    override def run()(implicit systemProvider: ClassicActorSystemProvider): Unit = {
      val done = mappedSource.runWith(Sink.ignore)
      testProbe.ref ! StartObserved
      promiseToStop.completeWith(done)
    }

    private def process(i: Int): Future[Done] = {
      concat(i)
      testProbe.ref ! Consumed(i, strBuffer.toString)
      Future.successful(Done)
    }

    private[projection] def mappedSource()(implicit systemProvider: ClassicActorSystemProvider): Source[Done, _] = {
      src.via(killSwitch.flow).mapAsync(1)(i => process(i))
    }

    private def concat(i: Int) = {
      if (strBuffer.toString.isEmpty) strBuffer.append(i)
      else strBuffer.append("-").append(i)
    }

    override def stop()(implicit ec: ExecutionContext): Future[Done] = {
      if (failToStop) {
        // this simulates a failure when stopping the stream
        // for the ProjectionBehavior the effect is the same
        Future.failed(new RuntimeException("failed to stop properly"))
      } else {
        killSwitch.shutdown()
        val stopFut = promiseToStop.future
        stopFut.foreach(_ => testProbe.ref ! StopObserved)
        stopFut
      }
    }
  }

}
