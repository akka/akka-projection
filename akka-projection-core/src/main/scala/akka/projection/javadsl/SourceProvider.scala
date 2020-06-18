/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationSuccess
import akka.stream.javadsl.Source
import com.github.ghik.silencer.silent

abstract class SourceProvider[Offset, Envelope] {

  def source(offset: Supplier[CompletionStage[Optional[Offset]]]): CompletionStage[Source[Envelope, _]]

  def extractOffset(envelope: Envelope): Offset

  @silent("never used")
  def verifyOffset(offset: Offset): OffsetVerification = VerificationSuccess

  def isOffsetMergeable: Boolean = false

}
