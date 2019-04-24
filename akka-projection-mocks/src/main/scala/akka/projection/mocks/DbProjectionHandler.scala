/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mocks

import akka.Done
import akka.projection.scaladsl.ProjectionHandler

import scala.util.Try
import scala.util.control.NonFatal

trait DbProjectionHandler[E] extends ProjectionHandler[E, DBIO[Done]] {

  override def onFailure(event: E, throwable: Throwable): DBIO[Done] = throw throwable

  override final def onEvent(event: E): DBIO[Done] =
    Try(handleEvent(event)).recover {
      case NonFatal(ex) => onFailure(event, ex)
    }.get
}
