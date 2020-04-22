/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.cassandra.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.annotation.ApiMayChange
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.cassandra.internal.CassandraProjectionImpl
import akka.stream.scaladsl.Source

@ApiMayChange
object CassandraProjection {

  def atLeastOnce[Offset, StreamElement](
      projectionId: ProjectionId,
      sourceProvider: Option[Offset] => Source[StreamElement, _],
      offsetExtractor: StreamElement => Offset,
      saveOffsetAfterElements: Int,
      saveOffsetAfterDuration: FiniteDuration)(handler: StreamElement => Future[Done]): Projection[StreamElement] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      offsetExtractor,
      saveOffsetAfterElements,
      saveOffsetAfterDuration,
      handler)
}
