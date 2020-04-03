/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange

import scala.concurrent.{ ExecutionContext, Future }

@ApiMayChange
trait Projection {
  def start()(implicit systemProvider: ClassicActorSystemProvider): Unit
  def stop(): Future[Done]
}
