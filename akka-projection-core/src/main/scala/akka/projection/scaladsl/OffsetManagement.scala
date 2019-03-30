/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.Done

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds

trait OffsetManagement[Offset, IO] {

  def readOffset(name: String): Future[Option[Offset]]

  def run(name: String, offset: Offset)
                   (block: => IO)
                   (implicit ec: ExecutionContext): Future[Done]

}
