/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.Done

import scala.concurrent.Future

class ProjectionHandler[Event](val eventHandler: Event => Future[Done])
