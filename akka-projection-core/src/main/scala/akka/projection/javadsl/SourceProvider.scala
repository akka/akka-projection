/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.function.{ Function => JFunction }
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import akka.NotUsed
import akka.annotation.InternalApi
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
 * by defining the `startOffset` function.
 */
abstract class CustomStartOffsetSourceProvider[Offset, Envelope] extends SourceProvider[Offset, Envelope] {

  /**
   * Adjust the start offset by defining a function that returns the offset to use.
   *
   * @param loadFromOffsetStore whether the offset should be loaded from the offset store or not, loaded offset
   *                            is passed as the parameter to the `startOffset` function
   * @param startOffset function from loaded offset (if any) to the adjusted offset
   */
  def withStartOffset(
      loadFromOffsetStore: Boolean,
      startOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]]): Unit

  def loadFromOffsetStore: Boolean

  def startOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]]
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait CustomStartOffsetSourceProviderImpl[Offset, Envelope]
    extends CustomStartOffsetSourceProvider[Offset, Envelope] {
  private var _loadFromOffsetStore: Boolean = true
  private var _startOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]] = offset =>
    CompletableFuture.completedFuture(offset)

  final override def withStartOffset(
      loadFromOffsetStore: Boolean,
      startOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]]): Unit = {
    _loadFromOffsetStore = loadFromOffsetStore
    _startOffset = startOffset
  }

  final override def loadFromOffsetStore: Boolean = _loadFromOffsetStore

  final def startOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]] = _startOffset
}

trait VerifiableSourceProvider[Offset, Envelope] extends SourceProvider[Offset, Envelope] {

  def verifyOffset(offset: Offset): OffsetVerification

}

trait MergeableOffsetSourceProvider[Offset <: MergeableOffset[_], Envelope] extends SourceProvider[Offset, Envelope]
