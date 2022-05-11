/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

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
import akka.projection.internal.AtMostOnce
import akka.projection.internal.HandlerStrategy
import akka.projection.internal.ManagementState
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.OffsetStrategy
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.ProjectionManagement
import akka.projection.scaladsl.SourceProvider
import akka.projection.testkit.internal.TestInMemoryOffsetStoreImpl
import akka.projection.testkit.internal.TestInternalProjectionState
import akka.projection.testkit.internal.TestProjectionImpl
import akka.projection.testkit.internal.TestRunningProjection
import akka.projection.testkit.scaladsl.TestOffsetStore
import akka.projection.testkit.scaladsl.TestSourceProvider
import akka.stream.OverflowStrategy
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike

object ProjectionBehaviorSpec {

  sealed trait ProbeMessage
  case object StartObserved extends ProbeMessage
  case class Consumed(n: Int, currentState: String) extends ProbeMessage
  case object StopObserved extends ProbeMessage

  private val TestProjectionId = ProjectionId("test-projection", "00")

  def handler(probe: TestProbe[ProbeMessage]): Handler[Int] = new Handler[Int] {
    val strBuffer: StringBuffer = new StringBuffer()
    override def process(env: Int): Future[Done] = {
      concat(env)
      probe.ref ! Consumed(env, strBuffer.toString)
      Future.successful(Done)
    }

    def concat(i: Int) = {
      if (strBuffer.toString.isEmpty) strBuffer.append(i)
      else strBuffer.append("-").append(i)
    }
  }

  /*
   * This TestProjection has a internal state that we can use to prove that on restart,
   * the actor is taking a new projection instance.
   */
  private[projection] object ProjectionBehaviourTestProjection {
    def apply(
        src: Source[Int, NotUsed],
        probe: TestProbe[ProbeMessage],
        projectionId: ProjectionId = TestProjectionId,
        failToStop: Boolean = false): ProjectionBehaviourTestProjection = {
      val handlerStrategy = new SingleHandlerStrategy[Int](() => handler(probe))
      val sourceProvider = TestSourceProvider(src, (i: Int) => i)
      new ProjectionBehaviourTestProjection(projectionId, sourceProvider, handlerStrategy, probe, failToStop)
    }
  }

  private[projection] class ProjectionBehaviourTestProjection(
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Int, Int],
      handlerStrategy: HandlerStrategy,
      testProbe: TestProbe[ProbeMessage],
      failToStop: Boolean)
      extends TestProjectionImpl[Int, Int](
        projectionId,
        sourceProvider,
        handlerStrategy,
        AtMostOnce(),
        NoopStatusObserver,
        () => new TestInMemoryOffsetStoreImpl[Int](),
        None) {

    override private[projection] def newState(implicit system: ActorSystem[_]): TestInternalProjectionState[Int, Int] =
      new ProjectionBehaviourTestInternalProjectionState(
        projectionId,
        sourceProvider,
        handlerStrategy,
        offsetStrategy,
        NoopStatusObserver,
        offsetStoreFactory(),
        testProbe,
        failToStop)

    private[projection] class ProjectionBehaviourTestInternalProjectionState(
        projectionId: ProjectionId,
        sourceProvider: SourceProvider[Int, Int],
        handlerStrategy: HandlerStrategy,
        offsetStrategy: OffsetStrategy,
        statusObserver: StatusObserver[Int],
        offsetStore: TestOffsetStore[Int],
        testProbe: TestProbe[ProbeMessage],
        failToStop: Boolean)(implicit system: ActorSystem[_])
        extends TestInternalProjectionState[Int, Int](
          projectionId,
          sourceProvider,
          handlerStrategy,
          offsetStrategy,
          statusObserver,
          offsetStore,
          None) {
      override def newRunningInstance(): RunningProjection =
        new ProjectionBehaviourTestRunningProjection(
          projectionId,
          mappedSource(),
          killSwitch,
          offsetStore,
          testProbe,
          failToStop)
    }

    private[projection] class ProjectionBehaviourTestRunningProjection(
        projectionId: ProjectionId,
        source: Source[Done, _],
        killSwitch: SharedKillSwitch,
        offsetStore: TestOffsetStore[Int],
        testProbe: TestProbe[ProbeMessage],
        failToStop: Boolean)(implicit _system: ActorSystem[_])
        extends TestRunningProjection(source, killSwitch)
        with RunningProjectionManagement[Int] {
      import system.executionContext

      override protected def run(): Future[Done] = {
        testProbe.ref ! StartObserved
        source.run()
      }

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
          .andThen {
            case _ => testProbe.ref ! StopObserved
          }
      }

      override def getOffset(): Future[Option[Int]] = {
        offsetStore.lastOffset() match {
          case Some(0) => Future.successful(None)
          case Some(n) => Future.successful(Some(n))
          case None    => Future.successful(None)
        }
      }

      override def setOffset(offset: Option[Int]): Future[Done] = {
        offset match {
          case None =>
            offsetStore.saveOffset(projectionId, 0)
            Future.successful(Done)
          case Some(n) =>
            if (n <= 3) {
              offsetStore.saveOffset(projectionId, n)
              Future.successful(Done)
            } else {
              import akka.actor.typed.scaladsl.adapter._
              import akka.pattern.after
              after(100.millis, system.toClassic.scheduler) {
                offsetStore.saveOffset(projectionId, n)
                Future.successful(Done)
              }
            }
        }
      }

      // RunningProjectionManagement
      override def getManagementState(): Future[Option[ManagementState]] =
        offsetStore.readManagementState(projectionId)

      // RunningProjectionManagement
      override def setPaused(paused: Boolean): Future[Done] =
        offsetStore.savePaused(projectionId, paused)

    }
  }
}
class ProjectionBehaviorSpec extends ScalaTestWithActorTestKit("""
  akka.projection.management.ask-timeout = 200 ms
  """) with AnyWordSpecLike with LogCapturing {

  import ProjectionBehavior.Internal._
  import ProjectionBehaviorSpec._

  private def setupTestProjection(
      projectionId: ProjectionId = TestProjectionId,
      earlyMgmtCommand: () => Unit = () => ())
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
    val projectionRef =
      testKit.spawn(ProjectionBehavior(ProjectionBehaviourTestProjection(src, testProbe, projectionId)))
    earlyMgmtCommand()
    eventually {
      srcRef.get() should not be null
    }
    (testProbe, projectionRef, srcRef)
  }

  "A ProjectionBehavior" must {

    "start immediately on demand" in {

      val testProbe = testKit.createTestProbe[ProbeMessage]()
      val src = Source(1 to 2)
      testKit.spawn(ProjectionBehavior(ProjectionBehaviourTestProjection(src, testProbe)))

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))
      // good, things are flowing

    }

    "stop after receiving stop message" in {

      val testProbe = testKit.createTestProbe[ProbeMessage]()
      val streamDoneProbe = testKit.createTestProbe[Done]()
      val src = Source(1 to 2).concat(Source.maybe).watchTermination()(Keep.right).mapMaterializedValue { done =>
        done.onComplete(_ => streamDoneProbe.ref ! Done)(system.executionContext)
        NotUsed
      }

      val projectionRef = testKit.spawn(ProjectionBehavior(ProjectionBehaviourTestProjection(src, testProbe)))

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))
      // good, things are flowing

      projectionRef ! ProjectionBehavior.Stop
      testProbe.expectMessage(StopObserved)
      streamDoneProbe.expectMessage(Done)

      testProbe.expectTerminated(projectionRef)
    }

    "stop after stopping actor without stop message" in {
      val testProbe = testKit.createTestProbe[ProbeMessage]()
      val streamDoneProbe = testKit.createTestProbe[Done]()
      val src = Source(1 to 2).concat(Source.maybe).watchTermination()(Keep.right).mapMaterializedValue { done =>
        done.onComplete(_ => streamDoneProbe.ref ! Done)(system.executionContext)
        NotUsed
      }

      val projectionRef = testKit.spawn(ProjectionBehavior(ProjectionBehaviourTestProjection(src, testProbe)))

      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(Consumed(1, "1"))
      testProbe.expectMessage(Consumed(2, "1-2"))
      // good, things are flowing

      testKit.stop(projectionRef)
      testProbe.expectMessage(StopObserved)
      streamDoneProbe.expectMessage(Done)

      createTestProbe().expectTerminated(projectionRef)

    }

    "also stop when stopping underlying stream results in failure" in {

      val testProbe = testKit.createTestProbe[ProbeMessage]()
      val src = Source(1 to 2)
      val projectionRef =
        testKit.spawn(ProjectionBehavior(ProjectionBehaviourTestProjection(src, testProbe, failToStop = true)))

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

    "support pause/resume" in {
      val (testProbe, projectionRef, _) = setupTestProjection()

      testProbe.expectMessage(StartObserved)

      val isPausedProbe = createTestProbe[Boolean]()
      projectionRef ! IsPaused(TestProjectionId, isPausedProbe.ref)
      isPausedProbe.expectMessage(false)

      val doneProbe = createTestProbe[Done]()
      projectionRef ! SetPaused(TestProjectionId, paused = true, doneProbe.ref)
      testProbe.expectMessage(StopObserved)
      doneProbe.expectMessage(Done)
      testProbe.expectMessage(StartObserved)

      projectionRef ! IsPaused(TestProjectionId, isPausedProbe.ref)
      isPausedProbe.expectMessage(true)

      projectionRef ! SetPaused(TestProjectionId, paused = false, doneProbe.ref)
      testProbe.expectMessage(StopObserved)
      doneProbe.expectMessage(Done)
      testProbe.expectMessage(StartObserved)

      projectionRef ! IsPaused(TestProjectionId, isPausedProbe.ref)
      isPausedProbe.expectMessage(false)
    }

    "handle pause/resume operations sequentially" in {
      val (testProbe, projectionRef, _) = setupTestProjection()

      testProbe.expectMessage(StartObserved)

      val isPausedProbe = createTestProbe[Boolean]()
      val pauseProbe = createTestProbe[Done]()
      val resumeProbe = createTestProbe[Done]()
      val currentOffsetProbe = createTestProbe[CurrentOffset[Int]]()
      val setOffsetProbe = createTestProbe[Done]()

      projectionRef ! IsPaused(TestProjectionId, isPausedProbe.ref)
      projectionRef ! GetOffset(TestProjectionId, currentOffsetProbe.ref)
      projectionRef ! SetPaused(TestProjectionId, paused = true, pauseProbe.ref)
      projectionRef ! SetOffset(TestProjectionId, Some(3), setOffsetProbe.ref)
      projectionRef ! GetOffset(TestProjectionId, currentOffsetProbe.ref)
      projectionRef ! SetPaused(TestProjectionId, paused = false, resumeProbe.ref)
      projectionRef ! GetOffset(TestProjectionId, currentOffsetProbe.ref)

      isPausedProbe.expectMessage(false)
      currentOffsetProbe.expectMessage(CurrentOffset[Int](TestProjectionId, None))
      testProbe.expectMessage(StopObserved)
      pauseProbe.expectMessage(Done)
      testProbe.expectMessage(StartObserved)
      testProbe.expectMessage(StopObserved)
      setOffsetProbe.expectMessage(Done)
      testProbe.expectMessage(StartObserved)
      currentOffsetProbe.expectMessage(CurrentOffset(TestProjectionId, Some(3)))
      testProbe.expectMessage(StopObserved)
      resumeProbe.expectMessage(Done)
      testProbe.expectMessage(StartObserved)
      currentOffsetProbe.expectMessage(CurrentOffset(TestProjectionId, Some(3)))
    }

    "work with ProjectionManagement extension" in {
      val projectionId1 = ProjectionId("test-projection-ext", "1")
      val (testProbe1, _, srcRef1) = setupTestProjection(projectionId1, earlyMgmtCommand = () => {
        // immediate request should work (via retries)
        ProjectionManagement(system).getOffset[Int](projectionId1).futureValue shouldBe None
      })

      val projectionId2 = ProjectionId("test-projection-ext", "2")
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
