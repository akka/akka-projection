/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.RestartBackoffSettings
import akka.projection.internal.SettingsImpl
import akka.projection.scaladsl.ProjectionManagement
import akka.stream.KillSwitches
import akka.stream.OverflowStrategy
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike

object ProjectionBehaviorSpec {

  sealed trait ProbeMessage
  case object StartObserved extends ProbeMessage
  case class Consumed(n: Int, currentState: String) extends ProbeMessage
  case object StopObserved extends ProbeMessage

  private val TestProjectionId = ProjectionId("test-projection", "00")

  /*
   * This TestProjection has a internal state that we can use to prove that on restart,
   * the actor is taking a new projection instance.
   */
  private[akka] case class TestProjection(
      src: Source[Int, NotUsed],
      testProbe: TestProbe[ProbeMessage],
      failToStop: Boolean = false,
      override val projectionId: ProjectionId = ProjectionBehaviorSpec.TestProjectionId)
      extends Projection[Int]
      with SettingsImpl[TestProjection] {

    private val offsetStore = new AtomicInteger

    private[akka] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = None

    override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection =
      new InternalProjectionState(testProbe, failToStop).newRunningInstance()

    override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, _] =
      new InternalProjectionState(testProbe, failToStop).mappedSource()

    override def withSettings(settings: ProjectionSettings): Projection[Int] =
      this // no need for ProjectionSettings in tests

    override val statusObserver: StatusObserver[Int] = NoopStatusObserver

    override def withStatusObserver(observer: StatusObserver[Int]): Projection[Int] =
      this // no need for StatusObserver in tests

    override def withRestartBackoffSettings(restartBackoff: RestartBackoffSettings): TestProjection = this
    override def withSaveOffset(afterEnvelopes: Int, afterDuration: FiniteDuration): TestProjection = this
    override def withGroup(groupAfterEnvelopes: Int, groupAfterDuration: FiniteDuration): TestProjection = this

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

        offsetStore.incrementAndGet()

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
        extends RunningProjection
        with ProjectionOffsetManagement[Int] {
      import system.executionContext

      testProbe.ref ! StartObserved
      private val futureDone = source.run()

      override def stop(): Future[Done] = {
        val stopFut =
          if (failToStop) {
            // this simulates a failure when stopping the stream
            // for the ProjectionBehavior the effect is the same
            Future.failed(new RuntimeException("failed to stop properly"))
          } else {
            killSwitch.shutdown()
            futureDone
          }
        // make sure the StopObserved is sent to testProbe before returned Future is completed
        stopFut
          .andThen { _ =>
            testProbe.ref ! StopObserved
          }
      }

      override def getOffset(): Future[Option[Int]] = {
        offsetStore.get() match {
          case 0 => Future.successful(None)
          case n => Future.successful(Some(n))
        }
      }

      override def setOffset(offset: Option[Int]): Future[Done] = {
        offset match {
          case None =>
            offsetStore.set(0)
            Future.successful(Done)
          case Some(n) =>
            if (n <= 3) {
              offsetStore.set(n)
              Future.successful(Done)
            } else {
              import akka.actor.typed.scaladsl.adapter._
              import akka.pattern.after
              after(100.millis, system.toClassic.scheduler) {
                offsetStore.set(n)
                Future.successful(Done)
              }
            }
        }

      }
    }

  }
}
class ProjectionBehaviorSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  import ProjectionBehavior.Internal._
  import ProjectionBehaviorSpec._

  private def setupTestProjection(projectionId: ProjectionId = TestProjectionId)
      : (TestProbe[ProbeMessage], ActorRef[ProjectionBehavior.Command], AtomicReference[ActorRef[Int]]) = {
    val srcRef = new AtomicReference[ActorRef[Int]]()
    import akka.actor.typed.scaladsl.adapter._
    val src =
      Source.actorRef(PartialFunction.empty, PartialFunction.empty, 10, OverflowStrategy.fail).mapMaterializedValue {
        ref =>
          srcRef.set(ref.toTyped)
          NotUsed
      }

    val testProbe = testKit.createTestProbe[ProbeMessage]()
    val projectionRef = testKit.spawn(ProjectionBehavior(TestProjection(src, testProbe, projectionId = projectionId)))
    eventually {
      srcRef.get() should not be null
    }
    (testProbe, projectionRef, srcRef)
  }

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

    "provide access to current offset" in {
      val (testProbe, projectionRef, srcRef) = setupTestProjection()
      srcRef.get() ! 1
      srcRef.get() ! 2

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))

      val currentOffsetProbe = createTestProbe[CurrentOffset[Int]]()

      projectionRef ! GetOffset(TestProjectionId, currentOffsetProbe.ref)
      currentOffsetProbe.expectMessage(CurrentOffset(TestProjectionId, Some(2)))

      srcRef.get() ! 3
      testProbe.expectMessage(Consumed(3, "1-2-3"))
      projectionRef ! GetOffset(TestProjectionId, currentOffsetProbe.ref)
      currentOffsetProbe.expectMessage(CurrentOffset(TestProjectionId, Some(3)))
    }

    "support update of current offset" in {
      val (testProbe, projectionRef, srcRef) = setupTestProjection()
      srcRef.get() ! 1
      srcRef.get() ! 2
      srcRef.get() ! 3

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))
      testProbe.expectMessage(Consumed(3, "1-2-3"))

      val currentOffsetProbe = createTestProbe[CurrentOffset[Int]]()

      projectionRef ! GetOffset(TestProjectionId, currentOffsetProbe.ref)
      currentOffsetProbe.expectMessage(CurrentOffset(TestProjectionId, Some(3)))

      val setOffsetProbe = createTestProbe[Done]()
      projectionRef ! SetOffset(TestProjectionId, Some(2), setOffsetProbe.ref)
      testProbe.expectMessage(StopObserved)
      setOffsetProbe.expectMessage(Done)
      testProbe.expectMessage(StartObserved)

      projectionRef ! GetOffset(TestProjectionId, currentOffsetProbe.ref)
      currentOffsetProbe.expectMessage(CurrentOffset(TestProjectionId, Some(2)))
    }

    "handle offset operations sequentially" in {
      val (testProbe, projectionRef, srcRef) = setupTestProjection()
      srcRef.get() ! 1
      srcRef.get() ! 2

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))

      val currentOffsetProbe = createTestProbe[CurrentOffset[Int]]()
      val setOffsetProbe = createTestProbe[Done]()

      projectionRef ! GetOffset(TestProjectionId, currentOffsetProbe.ref)
      projectionRef ! SetOffset(TestProjectionId, Some(3), setOffsetProbe.ref)
      projectionRef ! GetOffset(TestProjectionId, currentOffsetProbe.ref)
      projectionRef ! SetOffset(TestProjectionId, Some(5), setOffsetProbe.ref)
      projectionRef ! SetOffset(TestProjectionId, Some(7), setOffsetProbe.ref)
      projectionRef ! GetOffset(TestProjectionId, currentOffsetProbe.ref)

      currentOffsetProbe.expectMessage(CurrentOffset(TestProjectionId, Some(2)))
      currentOffsetProbe.expectMessage(CurrentOffset(TestProjectionId, Some(3)))
      currentOffsetProbe.expectMessage(CurrentOffset(TestProjectionId, Some(7)))

      testProbe.expectMessage(StopObserved)
      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(StopObserved)
      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(StopObserved)
      testProbe.expectMessage(StartObserved)
    }

    "work with ProjectionManagement extension" in {
      val projectionId1 = ProjectionId("test-projection-ext", "1")
      val projectionId2 = ProjectionId("test-projection-ext", "2")

      val (testProbe1, _, srcRef1) = setupTestProjection(projectionId1)
      val (testProbe2, _, srcRef2) = setupTestProjection(projectionId2)
      srcRef1.get() ! 1
      srcRef1.get() ! 2
      testProbe1.expectMessage(StartObserved)
      testProbe1.expectMessage(Consumed(1, "1"))
      testProbe1.expectMessage(Consumed(2, "1-2"))

      srcRef2.get() ! 1
      srcRef2.get() ! 2
      srcRef2.get() ! 3
      testProbe2.expectMessage(StartObserved)
      testProbe2.expectMessage(Consumed(1, "1"))
      testProbe2.expectMessage(Consumed(2, "1-2"))
      testProbe2.expectMessage(Consumed(3, "1-2-3"))

      ProjectionManagement(system).getOffset[Int](projectionId1).futureValue shouldBe Some(2)
      ProjectionManagement(system).getOffset[Int](projectionId2).futureValue shouldBe Some(3)

      ProjectionManagement(system).updateOffset[Int](projectionId1, 5).futureValue shouldBe Done
      ProjectionManagement(system).getOffset[Int](projectionId1).futureValue shouldBe Some(5)
      ProjectionManagement(system).getOffset[Int](projectionId2).futureValue shouldBe Some(3)
      testProbe1.expectMessage(StopObserved)
      testProbe1.expectMessage(StartObserved)
      testProbe2.expectNoMessage()

      ProjectionManagement(system).updateOffset[Int](projectionId2, 7).futureValue shouldBe Done
      ProjectionManagement(system).getOffset[Int](projectionId1).futureValue shouldBe Some(5)
      ProjectionManagement(system).getOffset[Int](projectionId2).futureValue shouldBe Some(7)
      testProbe2.expectMessage(StopObserved)
      testProbe2.expectMessage(StartObserved)
      testProbe1.expectNoMessage()

      ProjectionManagement(system).clearOffset(projectionId1).futureValue shouldBe Done
      ProjectionManagement(system).getOffset[Int](projectionId1).futureValue shouldBe None
      ProjectionManagement(system).getOffset[Int](projectionId2).futureValue shouldBe Some(7)
      testProbe1.expectMessage(StopObserved)
      testProbe1.expectMessage(StartObserved)
      testProbe2.expectNoMessage()
    }
  }

}
