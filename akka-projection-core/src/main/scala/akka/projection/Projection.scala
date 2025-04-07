/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.ManagementState
import akka.projection.internal.ProjectionSettings
import akka.stream.scaladsl.RestartSource
import akka.stream.scaladsl.Source

/**
 * The core abstraction in Akka Projections.
 *
 * A projection instance may share the same name and [[Envelope]], but must have a unique key. The key is used
 * to achieve processing parallelism for a projection.
 *
 * For example, many projections may share the same name "user-events-projection", but can process events for
 * different sharded entities within Akka Cluster, where key could be the Akka Cluster shardId.
 *
 * @tparam Envelope The envelope type of the projection.
 */
trait Projection[Envelope] {

  def projectionId: ProjectionId

  def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): Projection[Envelope]

  def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      maxRestarts: Int): Projection[Envelope]

  /**
   * Java API
   */
  def withRestartBackoff(
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double): Projection[Envelope]

  /**
   * Java API
   */
  def withRestartBackoff(
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      maxRestarts: Int): Projection[Envelope]

  def statusObserver: StatusObserver[Envelope]

  def withStatusObserver(observer: StatusObserver[Envelope]): Projection[Envelope]

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  @InternalApi
  private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, Future[Done]]

  /**
   * INTERNAL API
   */
  @InternalApi private[projection] def actorHandlerInit[T]: Option[ActorHandlerInit[T]]

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  @InternalApi
  private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection

}

/**
 * INTERNAL API
 *
 * Helper to wrap the projection source with a RestartSource using the provided settings.
 */
@InternalApi
private[projection] object RunningProjection {

  /**
   * When stopping an projection the retry mechanism is aborted via this exception.
   */
  case object AbortProjectionException extends RuntimeException("Projection aborted.") with NoStackTrace

  def withBackoff(source: () => Source[Done, _], settings: ProjectionSettings): Source[Done, _] = {
    RestartSource
      .onFailuresWithBackoff(settings.restartBackoff) { () =>
        source()
          .recoverWithRetries(1, {
            case AbortProjectionException => Source.empty // don't restart
          })
      }
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] trait RunningProjection {

  /**
   * Stop the projection if it's running.
   * @return Future[Done] - the returned Future should return the stream materialized value.
   */
  def stop(): Future[Done]
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] trait RunningProjectionManagement[Offset] {
  def getOffset(): Future[Option[Offset]]
  def setOffset(offset: Option[Offset]): Future[Done]
  def getManagementState(): Future[Option[ManagementState]]
  def setPaused(paused: Boolean): Future[Done]

  /** Not necessarily available for projections with all sources: future will fail with IllegalStateException
   *  if source does not support this operation
   */
  def getSourceMaxOffset(): Future[Option[Offset]]

  /** Computes the lag between the last saved offset for the projection and the max offset emittable by the
   *  source.
   *
   *  The returned future should fail with IllegalStateException if getSourceMaxOffset() fails with an
   *  IllegalStateException.
   *
   *  The returned future will complete with None if lag could not be computed now, but might be computable later.
   *  The returned future will complete positive if the saved offset is behind the source max offset.
   *  The returned future will complete zero if exactly caught up.
   *  The returned future will complete non-positive if the saved offset is ahead of max offset.
   */
  def getLag(): Future[Option[Long]]
}
