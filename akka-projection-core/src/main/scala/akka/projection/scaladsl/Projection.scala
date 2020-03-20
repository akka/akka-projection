/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.Done
import akka.stream.{ KillSwitch, KillSwitches, Materializer }
import akka.stream.scaladsl.{ Keep, Sink, Source }

import scala.concurrent.{ ExecutionContext, Future }

trait Projection {
  def start()(implicit ex: ExecutionContext, materializer: Materializer): Unit
  def stop()(implicit ex: ExecutionContext): Future[Done]
}