/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.Done

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.control.NonFatal

trait ProjectionHandler[Event, IO] {

  def handleEvent(event: Event): IO

  def onFailure(event: Event, throwable: Throwable): IO

  def onEvent(event: Event): IO
}

trait AsyncProjectionHandler[Event] extends ProjectionHandler[Event, Future[Done]]{

  implicit def exc: ExecutionContext

  def handleEvent(event: Event): Future[Done]

  def onFailure(event: Event, throwable: Throwable): Future[Done] =
    Future.failed(throwable)

  final def onEvent(event: Event): Future[Done] = {
    handleEvent(event)
      .recoverWith {
        case NonFatal(exp) => onFailure(event, exp)
      }
  }

}