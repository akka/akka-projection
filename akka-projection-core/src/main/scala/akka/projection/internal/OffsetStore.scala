/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.projection.ProjectionId

trait OffsetStore[Offset, T[_]] {
  def readOffset(projectionId: ProjectionId)(implicit ec: ExecutionContext): Future[Option[Offset]]
  def saveOffset(projectionId: ProjectionId, offset: Offset)(implicit ec: ExecutionContext): T[_]
  def saveOffsetAsync(projectionId: ProjectionId, offset: Offset)(implicit ec: ExecutionContext): Future[Done]
}
