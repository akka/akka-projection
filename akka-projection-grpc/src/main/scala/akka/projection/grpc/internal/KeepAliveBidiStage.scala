/*
 * Copyright (C) 2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import akka.annotation.InternalApi
import akka.projection.grpc.internal.proto.Ping
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.stream.Attributes
import akka.stream.BidiShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.TimerGraphStageLogic
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 *
 * akka-projection-grpc level keep alive for the consumer side of the gRPC `eventsBySlices` stream.
 *
 * Emits `Ping(id)` at `interval` and expects `Pong(id)` back. A watchdog scans in-flight pings
 * every `timeout / 4`; the stream fails after `failureThreshold` consecutive checks find the
 * oldest entry older than `timeout`. Below the threshold each overdue check logs a warning.
 * If `timeout` is `Duration.Zero` the watchdog is disabled — pings still flow to keep the
 * connection alive and RTT is logged at debug on Pong.
 */
@InternalApi
private[akka] final class KeepAliveBidiStage(
    interval: FiniteDuration,
    timeout: FiniteDuration,
    failureThreshold: Int,
    logPrefix: String)
    extends GraphStage[BidiShape[StreamIn, StreamIn, StreamOut, StreamOut]] {
  require(failureThreshold >= 1, s"failureThreshold must be >= 1, was [$failureThreshold]")

  private val inApp = Inlet[StreamIn]("KeepAliveBidi.inApp")
  private val outProducer = Outlet[StreamIn]("KeepAliveBidi.outProducer")
  private val inProducer = Inlet[StreamOut]("KeepAliveBidi.inProducer")
  private val outApp = Outlet[StreamOut]("KeepAliveBidi.outApp")

  override val shape: BidiShape[StreamIn, StreamIn, StreamOut, StreamOut] =
    BidiShape(inApp, outProducer, inProducer, outApp)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {
    private val log = LoggerFactory.getLogger(classOf[KeepAliveBidiStage])

    private val PingTickKey = "ping-tick"
    private val DeadlineCheckKey = "deadline-check"

    // Bounds the in-flight tracker when the watchdog is disabled (timeout = 0)
    private val MaxInFlight = 10

    private var nextId: Long = 0L
    private var inFlight = Map[Long, Long]()
    private var consecutiveOverdueChecks: Int = 0

    // At most one Ping is queued here when outProducer has no demand; newer ticks are dropped.
    private var pendingPing: Option[StreamIn] = None

    setHandler(inApp, new InHandler {
      override def onPush(): Unit = push(outProducer, grab(inApp))
      override def onUpstreamFinish(): Unit = complete(outProducer)
      override def onUpstreamFailure(ex: Throwable): Unit = fail(outProducer, ex)
    })

    setHandler(
      outProducer,
      new OutHandler {
        override def onPull(): Unit = {
          pendingPing match {
            case Some(p) =>
              pendingPing = None
              push(outProducer, p)
            case None =>
              if (isClosed(inApp)) {
                complete(outProducer)
              } else if (!hasBeenPulled(inApp)) {
                pull(inApp)
              }
          }
        }
        override def onDownstreamFinish(cause: Throwable): Unit = cancel(inApp, cause)
      })

    setHandler(
      inProducer,
      new InHandler {
        override def onPush(): Unit = {
          val msg = grab(inProducer)
          msg.message match {
            case StreamOut.Message.Pong(pong) =>
              inFlight.get(pong.id) match {
                case Some(sentAt) =>
                  inFlight -= pong.id
                  if (log.isDebugEnabled) {
                    val rttMs = (System.nanoTime() - sentAt) / 1000000L
                    log.debug("{}: Keepalive RTT for ping [{}]: [{} ms]", logPrefix, pong.id, rttMs)
                  }
                case None =>
                  log.debug("{}: Received Pong with unknown id [{}], ignoring", logPrefix, pong.id)
              }
              if (!hasBeenPulled(inProducer)) pull(inProducer)
            case _ =>
              push(outApp, msg)
          }
        }
        override def onUpstreamFinish(): Unit = complete(outApp)
        override def onUpstreamFailure(ex: Throwable): Unit = fail(outApp, ex)
      })

    setHandler(
      outApp,
      new OutHandler {
        override def onPull(): Unit = {
          if (isClosed(inProducer)) complete(outApp)
          else if (!hasBeenPulled(inProducer)) pull(inProducer)
        }
        override def onDownstreamFinish(cause: Throwable): Unit = cancel(inProducer, cause)
      })

    override def preStart(): Unit = {
      scheduleAtFixedRate(PingTickKey, interval, interval)
      if (timeout > Duration.Zero) {
        val checkPeriod = timeout / 4
        scheduleAtFixedRate(DeadlineCheckKey, checkPeriod, checkPeriod)
      }
    }

    override protected def onTimer(timerKey: Any): Unit = timerKey match {
      case PingTickKey =>
        val id = nextId
        nextId += 1
        val ping = StreamIn(StreamIn.Message.Ping(Ping(id)))

        val emitted =
          if (isAvailable(outProducer)) {
            push(outProducer, ping)
            true
          } else if (pendingPing.isEmpty) {
            pendingPing = Some(ping)
            true
          } else {
            log.debug("{}: Skipping Ping [{}] because outProducer is backed up", logPrefix, id)
            false
          }

        if (emitted) {
          inFlight += (id -> System.nanoTime())
          // Prune only with watchdog disabled; otherwise failStage would beat the cap.
          if (timeout <= Duration.Zero && inFlight.size > MaxInFlight) pruneInFlight()
        }

      case DeadlineCheckKey =>
        if (inFlight.isEmpty) {
          consecutiveOverdueChecks = 0
        } else {
          val (oldestId, oldestSentAt) = inFlight.minBy(_._2)
          val now = System.nanoTime()
          if (oldestSentAt < now - timeout.toNanos) {
            consecutiveOverdueChecks += 1
            val elapsedMs = (now - oldestSentAt) / 1000000L
            if (consecutiveOverdueChecks >= failureThreshold) {
              failStage(
                new TimeoutException(
                  s"$logPrefix: No keepalive Pong response for Ping [$oldestId] within [$timeout] " +
                  s"after [$failureThreshold] consecutive overdue checks (elapsed [$elapsedMs ms])"))
            } else {
              log.warn(
                "{}: Keepalive Pong for Ping [{}] overdue [{} ms > {}], consecutive overdue checks [{}/{}]",
                logPrefix,
                oldestId,
                elapsedMs,
                timeout,
                consecutiveOverdueChecks,
                failureThreshold)
            }
          } else {
            consecutiveOverdueChecks = 0
          }
        }

      case other =>
        log.warn("{}: Unexpected timer key [{}]", logPrefix, other)
    }

    private def pruneInFlight(): Unit = {
      val (oldestId, _) = inFlight.toSeq.minBy(_._2)
      log.debug("{}: Dropping in-flight Ping [{}] from tracker, cap reached", logPrefix, oldestId)
      inFlight -= oldestId
    }
  }

}
