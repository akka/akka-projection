/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.transactional

import akka.Done
import akka.projection.scaladsl.ProjectionHandler

trait DbProjectionHandler[E] extends ProjectionHandler[E, DBIO[Done]] {

  override def onFailure(event: E, throwable: Throwable): DBIO[Done] = throw throwable

  override def onEvent(event: E): DBIO[Done] = handleEvent(event)
}
