/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import akka.Done
import akka.annotation.ApiMayChange
import akka.projection.{ Projection, ProjectionId }
import akka.projection.slick.internal.SlickProjectionImpl
import akka.stream.scaladsl.Source
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.reflect.ClassTag

object SlickProjection {
  @ApiMayChange
  def transactional[Offset, Element, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: Option[Offset] => Source[Element, _],
      offsetExtractor: Element => Offset,
      databaseConfig: DatabaseConfig[P])(eventHandler: Element => DBIO[Done]): Projection[Element] =
    new SlickProjectionImpl(projectionId, sourceProvider, offsetExtractor, databaseConfig, eventHandler)
}
