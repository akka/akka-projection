/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.time.Duration
import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.Cancellable
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.projection.scaladsl.SourceProvider

// TODO: backlog status could be extended to track number of events as well or as an alternative for sequence-based offsets

/**
 * Represents the status of a projection's backlog - whether it's keeping up with events or lagging behind.
 */
@InternalStableApi
sealed trait BacklogStatus {

  /**
   * Indicates whether the projection is synchronized with the latest events.
   * @return true if the projection is in sync, false if lagging
   */
  def isInSync: Boolean
}

object BacklogStatus {

  /**
   * Base trait for statuses indicating the projection is synchronized with its event source.
   * The projection has processed all available events or there's nothing to process.
   */
  @InternalStableApi
  sealed trait InSync extends BacklogStatus {
    override def isInSync: Boolean = true
  }

  /**
   * No events to process and no processing has occurred, so the projection is up-to-date.
   */
  @InternalStableApi
  case object NoActivity extends InSync

  /**
   * Projection is up-to-date with the latest events, using timestamp comparison.
   * The latest processed offset timestamp is in-sync with the latest event timestamp.
   *
   * @param latestOffsetTimestamp the timestamp of the most recently processed event
   */
  @InternalStableApi
  final case class InSyncInTime(latestOffsetTimestamp: Instant) extends InSync

  /**
   * Base trait for statuses indicating the projection is lagging behind in processing events.
   * There are events in the source that haven't been processed yet.
   */
  @InternalStableApi
  sealed trait Lagging extends BacklogStatus {
    override def isInSync: Boolean = false
  }

  /**
   * Projection is lagging behind the latest events, measured in time difference.
   * The latest processed offset timestamp is behind the latest event timestamp.
   *
   * @param latestEventTimestamp  the timestamp of the most recent event in the source
   * @param latestOffsetTimestamp the timestamp of the most recently processed event
   */
  @InternalStableApi
  final case class LaggingInTime(latestEventTimestamp: Instant, latestOffsetTimestamp: Instant) extends Lagging {

    /**
     * The time difference between the latest event and the latest processed offset.
     */
    val lag: Duration = Duration.between(latestOffsetTimestamp, latestEventTimestamp)

    /**
     * The lag duration in seconds, with fractional precision.
     *
     * @return lag in seconds as a double
     */
    def lagSeconds: Double = lag.toNanos / 1_000_000_000.0
  }

  /**
   * There are events in the source, but the projection hasn't processed any events yet.
   * This represents an initial state or a projection that hasn't started processing.
   *
   * A telemetry implementation could track time since starting to process the current event,
   * in case the projection is taking a long time or has actually stalled on the first event.
   *
   * @param latestEventTimestamp the timestamp of the most recent event in the source
   */
  @InternalStableApi
  final case class NoProcessingInTime(latestEventTimestamp: Instant) extends Lagging

}

/**
 * Observing backlog status. Supported as an extension of [[Telemetry]].
 * Implementing this trait allows tracking whether projections are keeping up with events.
 */
@InternalStableApi
trait BacklogStatusTelemetry {

  /**
   * Define how frequently backlog status should be checked.
   * Return Duration.ZERO to disable backlog status checking.
   *
   * @return backlog status check interval as a duration
   */
  def backlogStatusCheckInterval(): Duration

  /**
   * Observe a reported backlog status.
   * Called periodically with the current backlog status of the projection.
   *
   * @param status latest backlog status
   */
  def reportBacklogStatus(status: BacklogStatus): Unit
}

object BacklogStatusTelemetry {

  /** INTERNAL API */
  @InternalApi
  def start[Offset, Envelope](
      telemetry: Telemetry,
      sourceProvider: SourceProvider[Offset, Envelope],
      projectionState: InternalProjectionState[Offset, Envelope])(
      implicit system: ActorSystem[_]): Option[Cancellable] = {
    for {
      backlogStatusTelemetry <- Option(telemetry).collect {
        case statusTelemetry: BacklogStatusTelemetry => statusTelemetry
      }
      checkInterval <- Option(backlogStatusTelemetry.backlogStatusCheckInterval()).filter { duration =>
        !duration.isZero && !duration.isNegative
      }
      backlogStatusSourceProvider <- Option(sourceProvider).collect {
        case provider: BacklogStatusSourceProvider if provider.supportsLatestEventTimestamp => provider
      }
      backlogStatusProjectionState <- Option(projectionState).collect {
        case state: BacklogStatusProjectionState if state.supportsLatestOffsetTimestamp => state
      }
    } yield {
      implicit val ec: ExecutionContext = system.executionContext
      system.scheduler.scheduleAtFixedRate(
        checkInterval,
        checkInterval,
        () => checkBacklogStatus(backlogStatusSourceProvider, backlogStatusProjectionState, backlogStatusTelemetry),
        system.executionContext)
    }
  }

  /** INTERNAL API */
  @InternalApi
  private def checkBacklogStatus(
      backlogStatusSourceProvider: BacklogStatusSourceProvider,
      backlogStatusProjectionState: BacklogStatusProjectionState,
      backlogStatusTelemetry: BacklogStatusTelemetry)(implicit ec: ExecutionContext): Unit = {

    val backlogStatus: Future[BacklogStatus] = for {
      latestEventTimestamp <- backlogStatusSourceProvider.latestEventTimestamp()
      latestOffsetTimestamp <- backlogStatusProjectionState.latestOffsetTimestamp()
    } yield calculateBacklogStatus(latestEventTimestamp, latestOffsetTimestamp)

    backlogStatus.foreach(backlogStatusTelemetry.reportBacklogStatus)
  }

  private def calculateBacklogStatus(
      latestEventTimestamp: Option[Instant],
      latestOffsetTimestamp: Option[Instant]): BacklogStatus = {
    import BacklogStatus._

    (latestEventTimestamp, latestOffsetTimestamp) match {
      case (Some(eventTimestamp), Some(offsetTimestamp)) =>
        // check whether projection is up-to-date
        // note: for an active projection, the offset may be ahead of events by the time this was checked
        if (offsetTimestamp.compareTo(eventTimestamp) >= 0) {
          InSyncInTime(offsetTimestamp)
        } else {
          LaggingInTime(eventTimestamp, offsetTimestamp)
        }

      case (Some(eventTimestamp), None) =>
        // there are events but none have been processed yet
        NoProcessingInTime(eventTimestamp)

      case (None, Some(offsetTimestamp)) =>
        // events have likely been deleted, but projection is up-to-date
        InSyncInTime(offsetTimestamp)

      case (None, None) =>
        // no events to process for this projection instance
        NoActivity
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
  private[akka] def supportsLatestOffsetTimestamp: Boolean
  private[akka] def latestOffsetTimestamp(): Future[Option[Instant]]
}
