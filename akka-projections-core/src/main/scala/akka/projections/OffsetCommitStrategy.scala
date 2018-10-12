/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projections

import akka.Done

import scala.concurrent.Future

trait OffsetCommitStrategy[+Envelope]

trait AatMostOnceCommitStrategy[E] extends OffsetCommitStrategy[E] {
  def saveOffset(envelope: E): Future[Done]
}
trait AtLeastOnceCommitStrategy[E] extends OffsetCommitStrategy[E] {
  def saveOffset(envelope: E): Future[Done]
}

case object InProjection extends OffsetCommitStrategy[Nothing]
