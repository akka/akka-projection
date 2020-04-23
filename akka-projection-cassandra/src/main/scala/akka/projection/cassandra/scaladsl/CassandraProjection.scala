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

  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: Option[Offset] => Source[Envelope, _],
      offsetExtractor: Envelope => Offset,
      saveOffsetAfterEnvelopes: Int,
      saveOffsetAfterDuration: FiniteDuration)(handler: Envelope => Future[Done]): Projection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      offsetExtractor,
      CassandraProjectionImpl.AtLeastOnce(saveOffsetAfterEnvelopes, saveOffsetAfterDuration),
      handler)

  def atMostOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: Option[Offset] => Source[Envelope, _],
      offsetExtractor: Envelope => Offset)(handler: Envelope => Future[Done]): Projection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      offsetExtractor,
      CassandraProjectionImpl.AtMostOnce,
      handler)
}
