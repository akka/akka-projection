/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import scala.concurrent.duration._

import akka.annotation.InternalApi
import akka.projection.grpc.internal.proto.StreamOut
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.TimerGraphStageLogic

/**
 * INTERNAL API
 */
@InternalApi private[akka] object KeepAliveStage {
  private case object CheckTimer

  // same shape as Akka's Timers.timeoutCheckInterval
  private def checkInterval(timeout: FiniteDuration): FiniteDuration =
    (timeout / 8).max(100.millis).min(1.second)
}

/**
 * INTERNAL API
 *
 * Drops keep alive messages from the producer and optionally fails the stream when the producer does not answer
 * demand within the timeout. Time is measured from pull to the next push, so a consumer that is busy and not
 * pulling never times out. The timeout is armed by the first keep alive, so producers without keep alive
 * support never trigger it.
 */
@InternalApi private[akka] final class KeepAliveStage(
    streamId: String,
    timeout: Option[FiniteDuration],
    correlationId: Option[String])
    extends GraphStage[FlowShape[StreamOut, StreamOut]] {
  import KeepAliveStage._

  private val in = Inlet[StreamOut]("KeepAliveStage.in")
  private val out = Outlet[StreamOut]("KeepAliveStage.out")
  override val shape: FlowShape[StreamOut, StreamOut] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      private var armed = false
      private var waitingForProducer = false
      private var deadline = 0L

      override def preStart(): Unit =
        timeout.foreach { t =>
          scheduleWithFixedDelay(CheckTimer, checkInterval(t), checkInterval(t))
        }

      override def onPull(): Unit = pullProducer()

      override def onPush(): Unit = {
        waitingForProducer = false
        val elem = grab(in)
        if (elem.message.isKeepAlive) {
          armed = true
          pullProducer()
        } else
          push(out, elem)
      }

      private def pullProducer(): Unit = {
        timeout.foreach(t => deadline = System.nanoTime() + t.toNanos)
        waitingForProducer = true
        pull(in)
      }

      override protected def onTimer(timerKey: Any): Unit =
        timeout.foreach { t =>
          if (armed && waitingForProducer && deadline - System.nanoTime() < 0)
            failStage(new KeepAliveTimeoutException(streamId, t, correlationId))
        }

      setHandlers(in, out, this)
    }
}
