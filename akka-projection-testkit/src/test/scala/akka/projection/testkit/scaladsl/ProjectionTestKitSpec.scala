/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.ProjectionId
import akka.projection.scaladsl.Handler
import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.DelayStrategy
import akka.stream.scaladsl.Source
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpecLike

class ProjectionTestKitSpec extends ScalaTestWithActorTestKit with LogCapturing with AnyWordSpecLike {

  val projectionTestKit: ProjectionTestKit = ProjectionTestKit(system)
  val projectionId: ProjectionId = ProjectionId("name", "key")

  def handler(strBuffer: StringBuffer, predicate: Int => Boolean): Handler[Int] = new Handler[Int] {
    override def process(env: Int): Future[Done] = {
      if (predicate(env)) concat(env)
      Future.successful(Done)
    }

    def concat(i: Int) = {
      if (strBuffer.toString.isEmpty) strBuffer.append(i)
      else strBuffer.append("-").append(i)
    }
  }

  "ProjectionTestKit" must {

    "assert progress of a projection" in {
      val strBuffer = new StringBuffer()
      val sp = TestSourceProvider(Source(1 to 20), (i: Int) => i)
      val prj = TestProjection(projectionId, sp, () => handler(strBuffer, _ <= 6))

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
      val sp = TestSourceProvider(delayedSrc, (i: Int) => i)
      val prj = TestProjection(projectionId, sp, () => handler(strBuffer, _ <= 6))

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
      val sp = TestSourceProvider(delayedSrc, (i: Int) => i)
      val prj = TestProjection(projectionId, sp, () => handler(strBuffer, _ <= 2))

      assertThrows[TestFailedException] {
        projectionTestKit.run(prj, max = 1.seconds) {
          strBuffer.toString shouldBe "1-2"
        }
      }
    }

    "failure inside Projection propagates to TestKit" in {
      val streamFailureMsg = "stream failure"
      val strBuffer = new StringBuffer()
      val predicate: Int => Boolean = {
        case envelope if envelope < 3 => true
        case _                        => throw new RuntimeException(streamFailureMsg)
      }
      val sp = TestSourceProvider(Source(1 to 20), (i: Int) => i)
      val prj = TestProjection(projectionId, sp, () => handler(strBuffer, predicate))

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
      val sp = TestSourceProvider(failingSource, (i: Int) => i)
      val prj = TestProjection(projectionId, sp, () => handler(strBuffer, _ <= 4))

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
      val sp = TestSourceProvider(Source(1 to 5), extractOffset = (i: Int) => i)
        .withAllowCompletion(true)
      val prj = TestProjection(projectionId, sp, () => handler(strBuffer, _ <= 5))

      projectionTestKit.runWithTestSink(prj) { sinkProbe =>
        sinkProbe.request(5)
        sinkProbe.expectNextN(5)
        sinkProbe.expectComplete()
      }

      strBuffer.toString shouldBe "1-2-3-4-5"
    }

  }

}
