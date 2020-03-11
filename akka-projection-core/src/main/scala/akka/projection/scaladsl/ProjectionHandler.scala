/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.language.higherKinds
import scala.util.control.NonFatal

trait ProjectionHandler[Event, IO] {

  def handleEvent(event: Event): IO

  def onFailure(event: Event, throwable: Throwable): IO

  def onEvent(event: Event): IO
}

trait AsyncProjectionHandler[Event, OffsetType] extends ProjectionHandler[Event, Future[Offset[OffsetType]]] {

  implicit def exc: ExecutionContext

  def handleEvent(event: Event): Future[Offset[OffsetType]]

  def onFailure(event: Event, throwable: Throwable): Future[Offset[OffsetType]] =
    Future.failed(throwable)

  final def onEvent(event: Event): Future[Offset[OffsetType]] =
    handleEvent(event)
      .recoverWith {
        case NonFatal(exp) => onFailure(event, exp)
      }

}
