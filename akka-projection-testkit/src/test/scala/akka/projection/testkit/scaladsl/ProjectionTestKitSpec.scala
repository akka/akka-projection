/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.scaladsl

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.stream.DelayOverflowStrategy
import akka.stream.KillSwitches
import akka.stream.scaladsl.DelayStrategy
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpecLike

class ProjectionTestKitSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  val projectionTestKit: ProjectionTestKit = ProjectionTestKit(testKit)

  "ProjectionTestKit" must {

    "assert progress of a projection" in {

      val strBuffer = new StringBuffer("")
      val prj = TestProjection(Source(1 to 20), strBuffer, _ <= 6)

      // stop as soon we observe that all expected elements passed through
      projectionTestKit.run(prj) {
        strBuffer.toString shouldBe "1-2-3-4-5-6"
      }
    }

    "retry assertion function until it succeeds within a max timeout" in {

      val strBuffer = new StringBuffer("")

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

      val strBuffer = new StringBuffer("")

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

      val strBuffer = new StringBuffer("")

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

      val strBuffer = new StringBuffer("")

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

      val strBuffer = new StringBuffer("")
      val projection = TestProjection(Source(1 to 5), strBuffer, _ <= 5)

      val sinkProbe = projectionTestKit.runWithTestSink(projection)

      sinkProbe.request(5)
      sinkProbe.expectNextN(5)
      sinkProbe.expectComplete()

      strBuffer.toString shouldBe "1-2-3-4-5"
    }
  }

  case class TestProjection(src: Source[Int, NotUsed], strBuffer: StringBuffer, predicate: Int => Boolean)
      extends Projection[Int] {

    override def projectionId: ProjectionId = ProjectionId("test-projection", "00")

    private val killSwitch = KillSwitches.shared(projectionId.id)
    private val promiseToStop = Promise[Done]

    override def run()(implicit systemProvider: ClassicActorSystemProvider): Unit = {
      val done =
        mappedSource.runWith(Sink.ignore)
      promiseToStop.completeWith(done)
    }

    private def process(elt: Int): Future[Done] = {
      if (predicate(elt)) concat(elt)
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
      killSwitch.shutdown()
      promiseToStop.future
    }
  }
}
