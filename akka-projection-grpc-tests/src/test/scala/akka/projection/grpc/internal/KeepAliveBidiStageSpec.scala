/*
 * Copyright (C) 2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import java.util.concurrent.TimeoutException

import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.grpc.internal.proto.Event
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.Ping
import akka.projection.grpc.internal.proto.Pong
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.stream.ClosedShape
import akka.stream.scaladsl.BidiFlow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.RunnableGraph
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import org.scalatest.wordspec.AnyWordSpecLike

class KeepAliveBidiStageSpec extends ScalaTestWithActorTestKit("""
    akka.loglevel = DEBUG
    akka.test.single-expect-default = 5s
    """) with AnyWordSpecLike with LogCapturing {

  private val pingInterval = 200.millis
  private val pongTimeout = 500.millis

  private def makeProbes(
      interval: FiniteDuration,
      timeout: FiniteDuration,
      logPrefix: String,
      failureThreshold: Int = 1): (
      TestPublisher.Probe[StreamIn],
      TestSubscriber.Probe[StreamIn],
      TestPublisher.Probe[StreamOut],
      TestSubscriber.Probe[StreamOut]) = {
    val bidi = BidiFlow.fromGraph(new KeepAliveBidiStage(interval, timeout, failureThreshold, logPrefix))
    RunnableGraph
      .fromGraph(
        GraphDSL
          .createGraph(TestSource[StreamIn](), TestSink[StreamIn](), TestSource[StreamOut](), TestSink[StreamOut]())(
            (a, b, c, d) => (a, b, c, d)) { implicit b => (appSrc, toProdSink, prodSrc, toAppSink) =>
            import GraphDSL.Implicits._
            val bidiShape = b.add(bidi)
            appSrc ~> bidiShape.in1
            bidiShape.out1 ~> toProdSink
            prodSrc ~> bidiShape.in2
            bidiShape.out2 ~> toAppSink
            ClosedShape
          })
      .run()
  }

  private def expectPing(probe: TestSubscriber.Probe[StreamIn], max: FiniteDuration): Ping =
    probe.expectNext(max).message match {
      case StreamIn.Message.Ping(p) => p
      case other                    => fail(s"Expected Ping, got [$other]")
    }

  private class Setup {
    val (appPub, toProdSub, prodPub, toAppSub) = makeProbes(pingInterval, pongTimeout, "test", failureThreshold = 1)
  }

  "KeepAliveBidiStage" must {

    "pass through non-ping StreamIn and non-pong StreamOut" in new Setup {
      toProdSub.request(10)
      toAppSub.request(10)

      val init = StreamIn(StreamIn.Message.Init(InitReq(streamId = "stream", sliceMin = 0, sliceMax = 1023)))
      appPub.sendNext(init)
      toProdSub.expectNext(init)

      val event = StreamOut(
        StreamOut.Message
          .Event(Event(persistenceId = "pid", seqNr = 1, slice = 0, offset = Nil, payload = None, source = "")))
      prodPub.sendNext(event)
      toAppSub.expectNext(event)
    }

    "emit Pings at the configured interval and accept Pongs" in new Setup {
      toProdSub.request(10)
      toAppSub.request(10)

      val p1 = expectPing(toProdSub, pingInterval + 1.second)
      prodPub.sendNext(StreamOut(StreamOut.Message.Pong(Pong(p1.id))))

      val p2 = expectPing(toProdSub, pingInterval + 1.second)
      prodPub.sendNext(StreamOut(StreamOut.Message.Pong(Pong(p2.id))))

      // Pongs were not forwarded to the app side
      toAppSub.expectNoMessage(100.millis)
    }

    "fail the stream when no Pong arrives within timeout" in new Setup {
      toProdSub.request(10)
      toAppSub.request(10)

      expectPing(toProdSub, pingInterval + 1.second)

      val err = toAppSub.expectError()
      err shouldBe a[TimeoutException]
      err.getMessage should include("No keepalive Pong response")
    }

    "keep emitting Pings without failing the stream when timeout is zero" in {
      val (_, toProdSub, _, toAppSub) = makeProbes(pingInterval, Duration.Zero, "test-no-timeout")
      toProdSub.request(10)
      toAppSub.request(10)

      // Several pings should be emitted, but no Pong responses, and the stream must not fail
      expectPing(toProdSub, pingInterval + 1.second)
      expectPing(toProdSub, pingInterval + 1.second)
      expectPing(toProdSub, pingInterval + 1.second)
      toAppSub.expectNoMessage(100.millis)
    }

    "continue forwarding events after handling a Pong" in new Setup {
      toProdSub.request(10)
      toAppSub.request(10)

      val p = expectPing(toProdSub, pingInterval + 1.second)
      prodPub.sendNext(StreamOut(StreamOut.Message.Pong(Pong(p.id))))

      val event = StreamOut(
        StreamOut.Message
          .Event(Event(persistenceId = "pid", seqNr = 1, slice = 0, offset = Nil, payload = None, source = "")))
      prodPub.sendNext(event)
      toAppSub.expectNext(1.second, event)
    }

    "ignore Pong with unknown id" in new Setup {
      toProdSub.request(10)
      toAppSub.request(10)

      prodPub.sendNext(StreamOut(StreamOut.Message.Pong(Pong(99999L))))
      toAppSub.expectNoMessage(100.millis)

      val p = expectPing(toProdSub, pingInterval + 1.second)
      prodPub.sendNext(StreamOut(StreamOut.Message.Pong(Pong(p.id))))
    }

    "fail on the deadline of the oldest in-flight Ping when only a later one is answered" in {
      val interval = 200.millis
      val timeout = 2.seconds
      val (_, toProdSub, prodPub, toAppSub) = makeProbes(interval, timeout, "test-partial")
      toProdSub.request(20)
      toAppSub.request(10)

      val p1 = expectPing(toProdSub, interval + 1.second)
      val p2 = expectPing(toProdSub, interval + 1.second)
      // consume the third ping from the probe; we deliberately don't answer it
      expectPing(toProdSub, interval + 1.second)

      // Only answer the middle one. p1 (the oldest) still has its deadline scheduled.
      prodPub.sendNext(StreamOut(StreamOut.Message.Pong(Pong(p2.id))))

      val err = toAppSub.expectError()
      err shouldBe a[TimeoutException]
      // The first unanswered ping (p1) is the oldest, its deadline fires first
      err.getMessage should include(s"Ping [${p1.id}]")
    }

    "queue at most one pending Ping when outProducer is backpressured" in {
      val (_, toProdSub, _, toAppSub) = makeProbes(100.millis, Duration.Zero, "test-pending")
      toAppSub.request(10)
      // intentionally do NOT request from toProdSub yet; several ticks will fire while there
      // is no demand. The first becomes pendingPing, the rest are dropped.
      Thread.sleep(500)

      // Grant single-element demand; the queued pendingPing arrives immediately and its id is 0
      // (the very first ping), proving the rest were dropped rather than buffered.
      toProdSub.request(1)
      val queued = expectPing(toProdSub, 200.millis)
      queued.id shouldBe 0L
    }

    "prune oldest in-flight Ping when MaxInFlight cap is reached" in {
      // timeout = 0 so the stream stays alive while in-flight grows
      val (_, toProdSub, _, toAppSub) = makeProbes(20.millis, Duration.Zero, "test-prune")
      toAppSub.request(50)
      // Bound demand at exactly 10: ticks 0..9 push, inFlight grows to 10 (== MaxInFlight, no prune yet).
      // Tick #10 finds no demand, becomes pendingPing, inFlight grows to 11 → exactly one prune fires.
      // Subsequent ticks find pendingPing already set and are dropped (no further inFlight growth).
      toProdSub.request(10)

      LoggingTestKit
        .debug("test-prune: Dropping in-flight Ping")
        .expect {
          for (_ <- 1 to 10) expectPing(toProdSub, 500.millis)
          // small grace window for the next tick (which becomes pendingPing) to fire and trigger the prune
          Thread.sleep(100)
        }
    }

    "correlate Pongs to Pings by id regardless of order" in {
      // Short interval, modest timeout. We answer the first three out of order, then keep
      // answering subsequent pings so the stream stays alive past where the originals
      // would have timed out if cancellation had failed.
      val interval = 50.millis
      val timeout = 500.millis
      val (_, toProdSub, prodPub, toAppSub) = makeProbes(interval, timeout, "test-ooo")
      toProdSub.request(50)
      toAppSub.request(10)

      val p1 = expectPing(toProdSub, interval + 1.second)
      val p2 = expectPing(toProdSub, interval + 1.second)
      val p3 = expectPing(toProdSub, interval + 1.second)

      // Reverse order: p3, p1, p2
      prodPub.sendNext(StreamOut(StreamOut.Message.Pong(Pong(p3.id))))
      prodPub.sendNext(StreamOut(StreamOut.Message.Pong(Pong(p1.id))))
      prodPub.sendNext(StreamOut(StreamOut.Message.Pong(Pong(p2.id))))

      // Keep answering subsequent pings so the stream is exercised past the original deadlines.
      // If any of p1/p2/p3's deadlines had not been cancelled, the stream would fail here.
      for (_ <- 1 to 15) {
        val p = expectPing(toProdSub, interval + 1.second)
        prodPub.sendNext(StreamOut(StreamOut.Message.Pong(Pong(p.id))))
      }

      // Stream still alive
      val event = StreamOut(
        StreamOut.Message
          .Event(Event(persistenceId = "pid", seqNr = 1, slice = 0, offset = Nil, payload = None, source = "")))
      prodPub.sendNext(event)
      toAppSub.expectNext(1.second, event)
    }

    "tolerate transient overdue Pongs up to failureThreshold and then fail" in {
      val interval = 200.millis
      val timeout = 500.millis
      // checkPeriod = timeout/4 = 125ms, so 3 overdue checks ≈ 375ms past the deadline
      val (_, toProdSub, _, toAppSub) =
        makeProbes(interval, timeout, "test-threshold", failureThreshold = 3)
      toProdSub.request(20)
      toAppSub.request(10)

      // First overdue check just warns; only the 3rd causes a failure. Total time until failure:
      // first ping sent ~200ms, deadline at ~700ms, then up to 3 * 125ms more = ~1075ms before fail.
      val err = LoggingTestKit
        .warn("overdue")
        .withOccurrences(2)
        .expect {
          val terminal = toAppSub.expectError()
          terminal shouldBe a[TimeoutException]
          terminal.getMessage should include("after [3] consecutive overdue checks")
          terminal
        }
      err.getMessage should include("after [3] consecutive overdue checks")
    }

    "propagate failure from inApp to outProducer" in new Setup {
      toProdSub.request(10)
      toAppSub.request(10)

      val cause = new RuntimeException("inApp failure")
      appPub.sendError(cause)

      toProdSub.expectError() shouldBe cause
    }

    "propagate failure from inProducer to outApp" in new Setup {
      toProdSub.request(10)
      toAppSub.request(10)

      val cause = new RuntimeException("inProducer failure")
      prodPub.sendError(cause)

      toAppSub.expectError() shouldBe cause
    }
  }
}
