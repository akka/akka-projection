/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import scala.concurrent.duration.FiniteDuration
import akka.Done
import akka.annotation.ApiMayChange
import akka.projection.scaladsl.SourceProvider
import akka.projection.{ Projection, ProjectionId }
import akka.projection.slick.internal.SlickProjectionImpl
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.reflect.ClassTag

@ApiMayChange
object SlickProjection {

  def exactlyOnce[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P])(eventHandler: Envelope => DBIO[Done]): Projection[Envelope] =
    new SlickProjectionImpl(projectionId, sourceProvider, databaseConfig, SlickProjectionImpl.ExactlyOnce, eventHandler)

  def atLeastOnce[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      saveOffsetAfterEnvelopes: Int,
      saveOffsetAfterDuration: FiniteDuration)(eventHandler: Envelope => DBIO[Done]): Projection[Envelope] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      SlickProjectionImpl.AtLeastOnce(saveOffsetAfterEnvelopes, saveOffsetAfterDuration),
      eventHandler)
}
