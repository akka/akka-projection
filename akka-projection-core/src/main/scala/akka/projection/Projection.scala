/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NoStackTrace

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
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

  def withSettings(settings: ProjectionSettings): Projection[Envelope]

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

  def withBackoff(source: () => Source[Done, _], settings: ProjectionSettings): Source[Done, _] =
    RestartSource
      .onFailuresWithBackoff(settings.minBackoff, settings.maxBackoff, settings.randomFactor, settings.maxRestarts) {
        () => source()
      }

  /* internal exception to wrap exceptions coming from stopHandler */
  private case class StopHandlerException(cause: Throwable) extends RuntimeException(cause) with NoStackTrace

  /**
   * Adds a `watchTermination` on the passed Source that will call the `stopHandler` on completion.
   *
   * The stopHandler function is called on success or failure. In case of failure, the original failure is preserved.
   */
  def stopHandlerOnTermination(src: Source[Done, NotUsed], stopHandler: () => Future[Done])(
      implicit ec: ExecutionContext): Source[Done, Future[Done]] = {
    src.watchTermination() { (_, futDone) =>
      futDone
        .flatMap { _ =>
          stopHandler().recoverWith {
            // if stop fails we need to wrap it so
            // on the next recoverWith we don't call it twice
            case exc => Future.failed(StopHandlerException(exc))
          }
        }
        .recoverWith {
          case StopHandlerException(exc) => Future.failed(exc)
          case streamFailure             =>
            // ignore error in stop failure and preserve original stream failure
            stopHandler().recoverWith(_ => Future.failed(streamFailure))
        }
        .map(_ => Done)
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
