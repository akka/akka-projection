/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object KeepAliveStageSpec {
  final class StageTimeout(timeout: FiniteDuration) extends RuntimeException(s"timeout after $timeout")
}

class KeepAliveStageSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with Matchers with LogCapturing {
  import KeepAliveStageSpec._

  // keep alives are 0, anything else is an element
  private val KeepAlive = 0

  private def run(
      timeout: Option[FiniteDuration],
      firstKeepAlives: AtomicInteger = new AtomicInteger,
      armedAtStart: Boolean = false) =
    TestSource[Int]()
      .via(
        Flow.fromGraph(
          new KeepAliveStage[Int](
            _ == KeepAlive,
            timeout,
            armedAtStart,
            new StageTimeout(_),
            () => firstKeepAlives.incrementAndGet())))
      .toMat(TestSink[Int]())(Keep.both)
      .run()

  "KeepAliveStage" must {

    "drop keep alives and pass other elements" in {
      val (in, out) = run(timeout = None)
      out.request(10)
      in.sendNext(KeepAlive)
      in.sendNext(1)
      in.sendNext(KeepAlive)
      in.sendNext(2)
      out.expectNext(1, 2)
      in.sendComplete()
      out.expectComplete()
    }

    "not fail without a timeout" in {
      val (in, out) = run(timeout = None)
      out.request(10)
      in.sendNext(KeepAlive)
      out.expectNoMessage(2.seconds)
      in.sendNext(3)
      out.expectNext(3)
      out.cancel()
    }

    "not fail before the first keep alive" in {
      val (in, out) = run(timeout = Some(1.second))
      out.request(10)
      // demand is outstanding but the peer has not shown keep alive support
      out.expectNoMessage(3.seconds)
      in.sendNext(4)
      out.expectNext(4)
      out.cancel()
    }

    "fail when the peer is silent after the first keep alive" in {
      val (in, out) = run(timeout = Some(1.second))
      out.request(10)
      in.sendNext(KeepAlive)
      out.within(3.seconds)(out.expectError()) shouldBe a[StageTimeout]
      in.expectCancellation()
    }

    "fail before the first keep alive when armed at start" in {
      val firstKeepAlives = new AtomicInteger
      val (in, out) = run(timeout = Some(1.second), firstKeepAlives, armedAtStart = true)
      out.request(10)
      out.within(3.seconds)(out.expectError()) shouldBe a[StageTimeout]
      in.expectCancellation()
      firstKeepAlives.get shouldBe 0
    }

    "call onFirstKeepAlive when armed at start" in {
      val firstKeepAlives = new AtomicInteger
      val (in, out) = run(timeout = Some(5.seconds), firstKeepAlives, armedAtStart = true)
      out.request(10)
      in.sendNext(KeepAlive)
      in.sendNext(8)
      out.expectNext(8)
      firstKeepAlives.get shouldBe 1
      out.cancel()
    }

    "reset the deadline on elements as well as keep alives" in {
      val (in, out) = run(timeout = Some(1.second))
      out.request(10)
      in.sendNext(KeepAlive)
      (1 to 6).foreach { n =>
        Thread.sleep(500)
        in.sendNext(n)
        out.expectNext(n)
      }
      out.expectNoMessage(500.millis)
      out.cancel()
    }

    "not fail while downstream has not pulled" in {
      val (in, out) = run(timeout = Some(1.second))
      out.request(1)
      in.sendNext(KeepAlive)
      in.sendNext(5)
      out.expectNext(5)
      // no demand, so the stage does not wait for the peer
      out.expectNoMessage(3.seconds)
      out.request(1)
      in.sendNext(6)
      out.expectNext(6)
      out.cancel()
    }

    "call onFirstKeepAlive once" in {
      val firstKeepAlives = new AtomicInteger
      val (in, out) = run(timeout = Some(5.seconds), firstKeepAlives)
      out.request(10)
      firstKeepAlives.get shouldBe 0
      in.sendNext(KeepAlive)
      in.sendNext(KeepAlive)
      in.sendNext(7)
      out.expectNext(7)
      firstKeepAlives.get shouldBe 1
      out.cancel()
    }

    "complete without failure when upstream completes while waiting" in {
      val (in, out) = run(timeout = Some(1.second))
      out.request(10)
      in.sendNext(KeepAlive)
      in.sendComplete()
      out.expectComplete()
      out.expectNoMessage(2.seconds)
    }

    "cancel upstream without failure when downstream cancels while waiting" in {
      val (in, out) = run(timeout = Some(1.second))
      out.request(10)
      in.sendNext(KeepAlive)
      out.cancel()
      in.expectCancellation()
    }
  }
}
