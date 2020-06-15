/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.StatusObserver
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.RestartBackoffSettings
import akka.projection.internal.SettingsImpl
import akka.stream.DelayOverflowStrategy
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.DelayStrategy
import akka.stream.scaladsl.Source
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpecLike

class ProjectionTestKitSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  val projectionTestKit: ProjectionTestKit = ProjectionTestKit(testKit)

  "ProjectionTestKit" must {

    "assert progress of a projection" in {

      val strBuffer = new StringBuffer()
      val prj = TestProjection(Source(1 to 20), strBuffer, _ <= 6)

      // stop as soon we observe that all expected elements passed through
      projectionTestKit.run(prj) {
        strBuffer.toString shouldBe "1-2-3-4-5-6"
      }
    }

    "retry assertion function until it succeeds within a max timeout" in {

      val strBuffer = new StringBuffer()

      // simulate slow stream by adding some delay on each element
      val delayedSrc = Source(1 to 20)
        .delayWith(() => DelayStrategy.linearIncreasingDelay(200.millis, _ => true), DelayOverflowStrategy.backpressure)

      val prj = TestProjection(delayedSrc, strBuffer, _ <= 6)

      // total processing time expected to be around 1.2 seconds
      projectionTestKit.run(prj, max = 2.seconds) {
        strBuffer.toString shouldBe "1-2-3-4-5-6"
      }
    }

    "retry assertion function and fail when timeout expires" in {

      val strBuffer = new StringBuffer()

      // simulate slow stream by adding some delay on each element
      val delayedSrc = Source(1 to 20).delayWith(
        () => DelayStrategy.linearIncreasingDelay(1000.millis, _ => true),
        DelayOverflowStrategy.backpressure)

      val prj = TestProjection(delayedSrc, strBuffer, _ <= 2)

      assertThrows[TestFailedException] {
        projectionTestKit.run(prj, max = 1.seconds) {
          strBuffer.toString shouldBe "1-2"
        }
      }
    }

    "failure inside Projection propagates to TestKit" in {

      val streamFailureMsg = "stream failure"

      val strBuffer = new StringBuffer()

      val prj = TestProjection(Source(1 to 20), strBuffer, {
        case envelope if envelope < 3 => true
        case _                        => throw new RuntimeException(streamFailureMsg)
      })

      val exp =
        intercept[RuntimeException] {
          projectionTestKit.run(prj) {
            strBuffer.toString shouldBe "1-2-3-4"
          }
        }

      exp.getMessage shouldBe streamFailureMsg
    }

    "failure inside Stream propagates to TestKit" in {

      val streamFailureMsg = "stream failure"

      val strBuffer = new StringBuffer()

      // this source will 'emit' an exception and fail the stream
      val failingSource = Source.single(1).concat(Source.failed(new RuntimeException(streamFailureMsg)))

      val prj = TestProjection(failingSource, strBuffer, _ <= 4)

      val exp =
        intercept[RuntimeException] {
          projectionTestKit.run(prj) {
            strBuffer.toString shouldBe "1-2-3-4"
          }
        }

      exp.getMessage shouldBe streamFailureMsg
    }

    "run a projection with a TestSink" in {

      val strBuffer = new StringBuffer()
      val projection = TestProjection(Source(1 to 5), strBuffer, _ <= 5)

      projectionTestKit.runWithTestSink(projection) { sinkProbe =>
        sinkProbe.request(5)
        sinkProbe.expectNextN(5)
        sinkProbe.expectComplete()
      }

      strBuffer.toString shouldBe "1-2-3-4-5"
    }

  }

  private[akka] case class TestProjection(src: Source[Int, NotUsed], strBuffer: StringBuffer, predicate: Int => Boolean)
      extends Projection[Int]
      with SettingsImpl[TestProjection] {

    override def withSettings(settings: ProjectionSettings): Projection[Int] =
      this // no need for ProjectionSettings in tests

    override val statusObserver: StatusObserver[Int] = NoopStatusObserver

    override def withStatusObserver(observer: StatusObserver[Int]): Projection[Int] =
      this // no need for StatusObserver in tests

    override def withRestartBackoffSettings(restartBackoff: RestartBackoffSettings): TestProjection = this
    override def withSaveOffset(afterEnvelopes: Int, afterDuration: FiniteDuration): TestProjection = this
    override def withGroup(groupAfterEnvelopes: Int, groupAfterDuration: FiniteDuration): TestProjection = this

    override def projectionId: ProjectionId = ProjectionId("test-projection", "00")

    private[akka] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = None

    override def run()(implicit system: ActorSystem[_]): RunningProjection =
      new InternalProjectionState(strBuffer, predicate).newRunningInstance()

    private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, _] =
      new InternalProjectionState(strBuffer, predicate).mappedSource()

    /*
     * INTERNAL API
     * This internal class will hold the KillSwitch that is needed
     * when building the mappedSource and when running the projection (to stop)
     */
    private class InternalProjectionState(strBuffer: StringBuffer, predicate: Int => Boolean)(
        implicit val system: ActorSystem[_]) {

      private val killSwitch = KillSwitches.shared(projectionId.id)

      def mappedSource(): Source[Done, _] =
        src.via(killSwitch.flow).mapAsync(1)(i => process(i))

      private def process(elt: Int): Future[Done] = {
        if (predicate(elt)) concat(elt)
        Future.successful(Done)
      }

      private def concat(i: Int) = {
        if (strBuffer.toString.isEmpty) strBuffer.append(i)
        else strBuffer.append("-").append(i)
      }

      def newRunningInstance(): RunningProjection =
        new TestRunningProjection(mappedSource(), killSwitch)
    }

    private class TestRunningProjection(val source: Source[Done, _], killSwitch: SharedKillSwitch)
        extends RunningProjection {

      private val futureDone = source.run()

      override def stop(): Future[Done] = {
        killSwitch.shutdown()
        futureDone
      }
    }
  }
}
