/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.Future

import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationSuccess
import akka.stream.scaladsl.Source
import com.github.ghik.silencer.silent

trait SourceProvider[Offset, Envelope] {

  def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, _]]

  def extractOffset(envelope: Envelope): Offset

  @silent("never used")
  def verifyOffset(offset: Offset): OffsetVerification = VerificationSuccess

  def isOffsetMergeable: Boolean = false

}
