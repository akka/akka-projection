/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.stream.scaladsl.Flow
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
  private[projection] def mappedSource()(implicit systemProvider: ClassicActorSystemProvider): Source[Done, _]

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  @InternalApi
  private[projection] def run()(implicit systemProvider: ClassicActorSystemProvider): RunningProjection

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

  def stopHandlerWhenStreamCompletedNormally(whenStreamCompleted: Future[Done], stopHandler: () => Future[Done])(
      implicit ec: ExecutionContext): Future[Done] = {
    whenStreamCompleted.flatMap(_ => stopHandler())
  }

  def stopHandlerWhenFailed[T](stopHandler: () => Future[Done])(implicit ec: ExecutionContext): Flow[T, T, _] = {
    Flow[T].recoverWithRetries(attempts = 1, {
      case exc =>
        Source
          .futureSource(stopHandler().recover(_ => Done).map(_ => Source.failed(exc)))
          .mapMaterializedValue(_ => NotUsed)
    })
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] trait RunningProjection {

  /**
   * INTERNAL API
   *
   * Stop the projection if it's running.
   * @return Future[Done] - the returned Future should return the stream materialized value.
   */
  @InternalApi
  private[projection] def stop()(implicit ec: ExecutionContext): Future[Done]
}
