/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.language.higherKinds

trait OffsetStore[OffsetType, IO] {
  def readOffset(): Future[Option[Offset[OffsetType]]]
  def saveOffset(offset: Offset[OffsetType]): IO
}

trait ProjectionRunner[OffsetType, IO] {

  def offsetStore: OffsetStore[Offset[OffsetType], IO]

  def run(offset: Offset[OffsetType])(handler: () => IO)(implicit ec: ExecutionContext): Future[Offset[OffsetType]]

}
