/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import akka.projection.MergeableKey
import akka.projection.MergeableOffset
import akka.NotUsed
import akka.projection.OffsetVerification

abstract class SourceProvider[Offset, Envelope] {

  def source(offset: Supplier[CompletionStage[Optional[Offset]]])
      : CompletionStage[akka.stream.javadsl.Source[Envelope, NotUsed]]

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
