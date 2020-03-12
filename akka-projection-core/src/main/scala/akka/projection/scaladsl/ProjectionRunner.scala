/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.Done

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds


trait ProjectionRunner[Offset, Result] {

  def offsetStore: OffsetStore[Offset, Result]

  def run(offset: Offset)
         (handler: () => Result)
         (implicit ec: ExecutionContext): Future[Done]

}

trait OffsetStore[Offset, Result] {
  def readOffset(): Future[Option[Offset]]
  def saveOffset(offset: Offset): Result
}
