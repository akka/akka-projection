/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.Future

import akka.projection.MergeableKey
import akka.projection.MergeableOffset
import akka.NotUsed
import akka.projection.OffsetVerification
import akka.stream.scaladsl.Source

trait SourceProvider[Offset, Envelope] {

  def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]]

  def extractOffset(envelope: Envelope): Offset

}

trait VerifiableSourceProvider[Offset, Envelope] extends SourceProvider[Offset, Envelope] {

  def verifyOffset(offset: Offset): OffsetVerification

}

trait MergeableOffsetSourceProvider[MKey <: MergeableKey, Offset <: MergeableOffset[MKey, _], Envelope]
    extends SourceProvider[Offset, Envelope]

trait CreationTimeSourceProvider[Offset, Envelope] extends SourceProvider[Offset, Envelope] {

  def extractCreationTime(envelope: Envelope): Long

}
