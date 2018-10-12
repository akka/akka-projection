/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projections

import akka.Done

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait Projection[E] {

  def handleEvent(element: E): Future[Done]

  def onFailure(element: E, throwable: Throwable): Future[Done] =
    Future.failed(throwable)

  final def onEvent(element: E)(implicit ex: ExecutionContext): Future[Done] = {
    handleEvent(element)
      .recoverWith {
        case NonFatal(exp) => onFailure(element, exp)
      }
  }

}
