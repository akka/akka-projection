/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.actor.ClassicActorSystemProvider
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.Projection
import akka.stream.scaladsl.{ DelayStrategy, Keep, Sink, Source }
import akka.stream.{ DelayOverflowStrategy, KillSwitch, KillSwitches }
import akka.{ Done, NotUsed }
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

class ProjectionTestKitSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  val projectionTestKit: ProjectionTestKit = ProjectionTestKit(testKit)

  "ProjectionTestKit" must {

    "assert progress of a projection" in {

      val concatenatedRef = new StringBuffer("")
      val src =
        Source(1 to 20)
      val prj = TestProjection(src, concatenatedRef, _ <= 6)

      // stop as soon we observe that all expected elements passed through
      projectionTestKit.run(prj) {
        concatenatedRef.toString shouldBe "1-2-3-4-5-6"
      }
    }

    "retry assertion function until it succeeds within a max timeout" in {

      val concatenatedRef = new StringBuffer("")

      // simulate slow stream by adding some delay on each element
      val src =
        Source(1 to 20).delayWith(
          () => DelayStrategy.linearIncreasingDelay(200.millis, _ => true),
          DelayOverflowStrategy.backpressure)

      val prj = TestProjection(src, concatenatedRef, _ <= 6)

      // total processing time expected to be around 1.2 seconds
      projectionTestKit.run(prj, max = 2.seconds) {
        concatenatedRef.toString shouldBe "1-2-3-4-5-6"
      }
    }

    "retry assertion function and fail if timeout expires" in {

      val concatenatedRef = new StringBuffer("")

      // simulate slow stream by adding some delay on each element
      val src =
        Source(1 to 20).delayWith(
          () => DelayStrategy.linearIncreasingDelay(1000.millis, _ => true),
          DelayOverflowStrategy.backpressure)

      val prj = TestProjection(src, concatenatedRef, _ <= 2)

      assertThrows[TestFailedException] {
        projectionTestKit.run(prj, max = 1.seconds) {
          concatenatedRef.toString shouldBe "1-2"
        }
      }
    }

    "failure inside Projection propagates to TestKit" in {

      val streamFailureMsg = "stream failure"

      val concatenatedRef = new StringBuffer("")
      val src = Source(1 to 20)

      val prj = TestProjection(src, concatenatedRef, {
        case elt if elt < 3 => true
        case _              => throw new RuntimeException(streamFailureMsg)
      })

      val exp =
        intercept[RuntimeException] {
          projectionTestKit.run(prj) {
            concatenatedRef.toString shouldBe "1-2-3-4"
          }
        }

      exp.getMessage shouldBe streamFailureMsg
    }

    "failure inside Stream propagates to TestKit" in {

      val streamFailureMsg = "stream failure"

      val concatenatedRef = new StringBuffer("")

      // this source will 'emit' an exception and fail the stream
      val src = Source.single(1).concat(Source.failed(new RuntimeException(streamFailureMsg)))

      val prj = TestProjection(src, concatenatedRef, _ <= 4)

      val exp =
        intercept[RuntimeException] {
          projectionTestKit.run(prj) {
            concatenatedRef.toString shouldBe "1-2-3-4"
          }
        }

      exp.getMessage shouldBe streamFailureMsg
    }
  }

  case class TestProjection(src: Source[Int, NotUsed], concatenatedRef: StringBuffer, eltPredicate: Int => Boolean)
      extends Projection[Int] {

    override def name: String = "test-projection"
    override def key: String = "00"

    private var shutdown: Option[KillSwitch] = None
    private val promiseToStop = Promise[Done]

    private def concat(elt: Int) = {
      if (concatenatedRef.toString == "") concatenatedRef.append(elt)
      else concatenatedRef.append("-" + elt)
    }

    override def processElement(elt: Int): Future[Int] = {
      if (eltPredicate(elt)) concat(elt)
      Future.successful(elt)
    }
    override def start()(implicit systemProvider: ClassicActorSystemProvider): Unit = {

      implicit val ec = systemProvider.classicSystem.dispatcher

      val (killSwitch, sinkMat) =
        src
          .viaMat(KillSwitches.single)(Keep.right)
          .mapAsync(1) { elt => processElement(elt).map(_ => Done) }
          .toMat(Sink.ignore)(Keep.both)
          .run()

      shutdown = Some(killSwitch)
      promiseToStop.completeWith(sinkMat)
    }

    override def stop(): Future[Done] = {
      shutdown.foreach(_.shutdown())
      promiseToStop.future
    }
  }
}
