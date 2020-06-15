/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NoStackTrace

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.ProjectionSettings
import akka.projection.scaladsl.HandlerLifecycle
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
@ApiMayChange
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
   */
  @InternalApi private[akka] def withSettings(settings: ProjectionSettings): Projection[Envelope]

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  @InternalApi
  private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, _]

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def actorHandlerInit[T]: Option[ActorHandlerInit[T]]

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
    val backoff = settings.restartBackoff
    RestartSource
      .onFailuresWithBackoff(backoff.minBackoff, backoff.maxBackoff, backoff.randomFactor, backoff.maxRestarts) { () =>
        source()
          .recoverWithRetries(1, {
            case AbortProjectionException => Source.empty // don't restart
          })
      }
  }

  /**
   * Adds a `watchTermination` on the passed Source that will call the `stopHandler` on completion.
   *
   * The stopHandler function is called on success or failure. In case of failure, the original failure is preserved.
   */
  def stopHandlerOnTermination(
      src: Source[Done, NotUsed],
      projectionId: ProjectionId,
      handlerLifecycle: HandlerLifecycle,
      statusObserver: StatusObserver[_])(implicit ec: ExecutionContext): Source[Done, Future[Done]] = {
    src.watchTermination() { (_, futDone) =>
      futDone
        .andThen(_ => handlerLifecycle.tryStop())
        .andThen {
          case Success(_) =>
            statusObserver.stopped(projectionId)
          case Failure(AbortProjectionException) =>
            statusObserver.stopped(projectionId) // no restart
          case Failure(exc) =>
            Try(statusObserver.stopped(projectionId))
            statusObserver.failed(projectionId, exc)
        }
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
private[projection] trait ProjectionOffsetManagement[Offset] {
  def getOffset(): Future[Option[Offset]]
  def setOffset(offset: Option[Offset]): Future[Done]
}
