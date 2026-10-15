/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import scala.concurrent.duration._

import akka.annotation.InternalApi
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
 * Drops keep alive messages from the other side and optionally fails the stream when the other side does not
 * answer demand within the timeout. Time is measured from pull to the next push, so a downstream that is busy
 * and not pulling never causes a timeout. The timeout is armed by the first keep alive, so a peer without keep
 * alive support never triggers it. Used on both the consumer side (StreamOut) and the producer side (StreamIn).
 *
 * The wait is measured in timer ticks rather than clock reads, to keep the pull and push paths free of
 * `System.nanoTime`, so the timeout is accurate to within one check interval.
 */
@InternalApi private[akka] final class KeepAliveStage[T](
    isKeepAlive: T => Boolean,
    timeout: Option[FiniteDuration],
    timeoutFailure: FiniteDuration => Throwable,
    onFirstKeepAlive: () => Unit)
    extends GraphStage[FlowShape[T, T]] {
  import KeepAliveStage._

  private val in = Inlet[T]("KeepAliveStage.in")
  private val out = Outlet[T]("KeepAliveStage.out")
  override val shape: FlowShape[T, T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      private var armed = false
      private var waitingForPeer = false
      // ticks since the pull, plus one since the first tick may come right after the pull
      private var waitedTicks = 0
      private val timeoutTicks = timeout.fold(0) { t =>
        val interval = checkInterval(t)
        ((t.toNanos + interval.toNanos - 1) / interval.toNanos).toInt + 1
      }

      override def preStart(): Unit =
        timeout.foreach { t =>
          scheduleWithFixedDelay(CheckTimer, checkInterval(t), checkInterval(t))
        }

      override def onPull(): Unit = pullPeer()

      override def onPush(): Unit = {
        waitingForPeer = false
        val elem = grab(in)
        if (isKeepAlive(elem)) {
          if (!armed) {
            armed = true
            onFirstKeepAlive()
          }
          pullPeer()
        } else
          push(out, elem)
      }

      private def pullPeer(): Unit = {
        waitedTicks = 0
        waitingForPeer = true
        pull(in)
      }

      override protected def onTimer(timerKey: Any): Unit =
        if (armed && waitingForPeer) {
          waitedTicks += 1
          if (waitedTicks >= timeoutTicks)
            failStage(timeoutFailure(timeout.get))
        }

      setHandlers(in, out, this)
    }
}
