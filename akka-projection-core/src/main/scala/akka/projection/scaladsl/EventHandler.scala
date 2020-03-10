/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.Done

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.control.NonFatal

trait EventHandler[Event, Result] {

  def handleEvent(event: Event): Result

  def onFailure(event: Event, throwable: Throwable): Result

  def onEvent(event: Event): Result
}

trait AsyncEventHandler[Event] extends EventHandler[Event, Future[Done]]{

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