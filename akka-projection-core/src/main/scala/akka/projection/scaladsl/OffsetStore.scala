/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.Done

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds

trait OffsetStore[Offset, IO] {
  def readOffset(): Future[Option[Offset]]
  def saveOffset(offset: Offset): IO
}

trait ProjectionRunner[Offset, IO] {

  def offsetStore: OffsetStore[Offset, IO]

  def run(offset: Offset)
         (handler: () => IO)
         (implicit ec: ExecutionContext): Future[Done]

}
