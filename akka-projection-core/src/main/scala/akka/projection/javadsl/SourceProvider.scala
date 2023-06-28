/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier
import java.util.function.{ Function => JFunction }

import akka.NotUsed
import akka.projection.MergeableOffset
import akka.projection.OffsetVerification

abstract class SourceProvider[Offset, Envelope] {

  def source(offset: Supplier[CompletionStage[Optional[Offset]]])
      : CompletionStage[akka.stream.javadsl.Source[Envelope, NotUsed]]

  def extractOffset(envelope: Envelope): Offset

  /**
   * Timestamp (in millis-since-epoch) of the instant when the envelope was created. The meaning of "when the
   * envelope was created" is implementation specific and could be an instant on the producer machine, or the
   * instant when the database persisted the envelope, or other.
   */
  def extractCreationTime(envelope: Envelope): Long

}

/**
 * By default, a `SourceProvider` uses the stored offset when starting the Projection. This offset can be adjusted
 * by decorating the `SourceProvider` with this class and defining the `adjustStartOffset` function.
 *
 * @param loadFromOffsetStore whether the offset should be loaded from the offset store or not, loaded offset
 *                            is passed as the parameter to the `startOffset` function
 * @param adjustStartOffset function from loaded offset (if any) to the adjusted offset
 * @param delegate the original `SourceProvider`
 */
final class CustomStartOffsetSourceProvider[Offset, Envelope](
    val loadFromOffsetStore: Boolean,
    val adjustStartOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]],
    val delegate: SourceProvider[Offset, Envelope])
    extends SourceProvider[Offset, Envelope] {

  override def source(fromOffsetStore: Supplier[CompletionStage[Optional[Offset]]])
      : CompletionStage[akka.stream.javadsl.Source[Envelope, NotUsed]] = {
    val startOffsetFn: Supplier[CompletionStage[Optional[Offset]]] = {
      if (loadFromOffsetStore) { () =>
        fromOffsetStore.get().thenCompose { offset =>
          adjustStartOffset(offset)
        }
      } else { () =>
        adjustStartOffset(Optional.empty[Offset])
      }
    }

    delegate.source(startOffsetFn)
  }

  override def extractOffset(envelope: Envelope): Offset =
    delegate.extractOffset(envelope)

  override def extractCreationTime(envelope: Envelope): Long =
    delegate.extractCreationTime(envelope)
}

trait VerifiableSourceProvider[Offset, Envelope] extends SourceProvider[Offset, Envelope] {

  def verifyOffset(offset: Offset): OffsetVerification

}

trait MergeableOffsetSourceProvider[Offset <: MergeableOffset[_], Envelope] extends SourceProvider[Offset, Envelope]
