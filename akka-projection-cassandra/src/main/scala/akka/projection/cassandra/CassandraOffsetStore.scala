/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.cassandra

import scala.concurrent.Future

import akka.Done
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.projection.ProjectionId

@ApiMayChange
@DoNotInherit
trait CassandraOffsetStore {

  // FIXME compared to slick.OffsetStore this has the Cassandra prefix in the name?

  def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]]

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): Future[Done]

  def createKeyspaceAndTable(): Future[Done]
}
