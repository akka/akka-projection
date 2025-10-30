/*
 * Copyright (C) 2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.internal

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.Cancellable
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.projection.scaladsl.SourceProvider

// TODO: backlog status could be extended to track number of events as well or as an alternative for sequence-based offsets

/**
 * Observing backlog status. Supported as an extension of [[Telemetry]].
 * Implementing this trait allows tracking whether projections are keeping up with their source.
 */
@InternalStableApi
trait BacklogStatusTelemetry {

  /**
   * Define how frequently backlog status should be checked.
   * Return 0 to disable backlog status checking.
   *
   * @return backlog status check interval in seconds
   */
  def backlogStatusCheckIntervalSeconds(): Int

  /**
   * Observe a reported backlog status, based on the latest source and offset timestamps.
   *
   * Timestamps are in milliseconds since epoch. Timestamps will be 0 when not present.
   *
   * The offset timestamp may be ahead of the source timestamp at the time of checking.
   * If only the source timestamp is present, then the projection has not yet processed the first envelope.
   * If only the offset timestamp is present, then the source may have been cleaned and the projection is up-to-date.
   * If neither timestamp is present, then there has been no activity and the projection is up-to-date.
   *
   * Called periodically, based on [[backlogStatusCheckIntervalSeconds]], with the backlog status of the projection.
   *
   * @param latestSourceTimestamp latest millisecond-based timestamp for the projection source (0 if not present)
   * @param latestOffsetTimestamp latest millisecond-based timestamp for projection offsets (0 if not present)
   */
  def reportTimestampBacklogStatus(latestSourceTimestamp: Long, latestOffsetTimestamp: Long): Unit
}

object BacklogStatusTelemetry {

  /** INTERNAL API */
  @InternalApi
  private[internal] def start[Offset, Envelope](
      telemetry: Telemetry,
      sourceProvider: SourceProvider[Offset, Envelope],
      projectionState: InternalProjectionState[Offset, Envelope])(implicit system: ActorSystem[_]): Seq[Cancellable] = {
    val backlogStatusTelemetries: Seq[BacklogStatusTelemetry] = telemetry match {
      case backlogStatusTelemetry: BacklogStatusTelemetry => Seq(backlogStatusTelemetry)
      case ensemble: EnsembleTelemetry =>
        ensemble.telemetries.collect {
          case backlogStatusTelemetry: BacklogStatusTelemetry => backlogStatusTelemetry
        }
      case _ => Seq.empty
    }
    backlogStatusTelemetries.flatMap { backlogStatusTelemetry =>
      startBacklogStatusChecks(backlogStatusTelemetry, sourceProvider, projectionState)
    }
  }

  private def startBacklogStatusChecks[Offset, Envelope](
      backlogStatusTelemetry: BacklogStatusTelemetry,
      sourceProvider: SourceProvider[Offset, Envelope],
      projectionState: InternalProjectionState[Offset, Envelope])(
      implicit system: ActorSystem[_]): Option[Cancellable] = {
    for {
      backlogStatusSourceProvider <- Option(sourceProvider).collect {
        case provider: BacklogStatusSourceProvider if provider.supportsLatestEventTimestamp => provider
      }
      backlogStatusProjectionState <- Option(projectionState).collect {
        case state: BacklogStatusProjectionState => state
      }
      checkInterval <- Option(backlogStatusTelemetry.backlogStatusCheckIntervalSeconds()).filter(_ > 0).map(_.seconds)
    } yield {
      implicit val ec: ExecutionContext = system.executionContext
      system.scheduler.scheduleAtFixedRate(checkInterval, checkInterval) { () =>
        checkBacklogStatus(backlogStatusSourceProvider, backlogStatusProjectionState, backlogStatusTelemetry)
      }
    }
  }

  private def checkBacklogStatus(
      backlogStatusSourceProvider: BacklogStatusSourceProvider,
      backlogStatusProjectionState: BacklogStatusProjectionState,
      backlogStatusTelemetry: BacklogStatusTelemetry)(implicit ec: ExecutionContext): Unit = {
    for {
      latestSourceTimestamp <- backlogStatusSourceProvider.latestEventTimestamp()
      latestOffsetTimestamp <- backlogStatusProjectionState.latestOffsetTimestamp()
    } {
      backlogStatusTelemetry.reportTimestampBacklogStatus(
        latestSourceTimestamp.fold(0L)(_.toEpochMilli),
        latestOffsetTimestamp.fold(0L)(_.toEpochMilli))
    }
  }
}

/** INTERNAL API */
@InternalApi
private[akka] trait BacklogStatusSourceProvider {
  private[akka] def supportsLatestEventTimestamp: Boolean
  private[akka] def latestEventTimestamp(): Future[Option[Instant]]
}

/** INTERNAL API */
@InternalApi
private[akka] trait BacklogStatusProjectionState {
  private[akka] def latestOffsetTimestamp(): Future[Option[Instant]]
}
