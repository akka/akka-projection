/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.Future

import akka.projection.MergeableOffset
import akka.NotUsed
import akka.projection.OffsetVerification
import akka.stream.scaladsl.Source

trait SourceProvider[Offset, Envelope] {

  def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]]

  def extractOffset(envelope: Envelope): Offset

  /**
   * Timestamp (in millis-since-epoch) of the instant when the envelope was created. The meaning of "when the
   * envelope was created" is implementation specific and could be an instant on the producer machine, or the
   * instant when the database persisted the envelope, or other.
   */
  def extractCreationTime(envelope: Envelope): Long

}

trait VerifiableSourceProvider[Offset, Envelope] extends SourceProvider[Offset, Envelope] {

  def verifyOffset(offset: Offset): OffsetVerification

}

trait MergeableOffsetSourceProvider[Offset <: MergeableOffset[_], Envelope] extends SourceProvider[Offset, Envelope]
