/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.javadsl.EventTimestampQuery
import akka.persistence.query.typed.javadsl.LoadEventQuery
import akka.persistence.query.typed.scaladsl.{ EventTimestampQuery => ScalaEventTimestampQuery }
import akka.persistence.query.typed.scaladsl.{ LoadEventQuery => ScalaLoadEventQuery }
import akka.projection.BySlicesSourceProvider
import akka.projection.javadsl
import akka.projection.scaladsl
import akka.stream.javadsl.{ Source => JSource }

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._
import scala.concurrent.ExecutionContext

/**
 * INTERNAL API: Adapter from scaladsl.SourceProvider with BySlicesSourceProvider to javadsl.SourceProvider with BySlicesSourceProvider
 */
@InternalApi private[projection] object ScalaToJavaBySlicesSourceProviderAdapter {
  def apply[Offset, Envelope](
      delegate: scaladsl.SourceProvider[Offset, Envelope]
        with BySlicesSourceProvider): javadsl.SourceProvider[Offset, Envelope] =
    delegate match {
      case adapted: JavaToScalaBySliceSourceProviderAdapter[_, _] =>
        // just unwrap rather than wrapping further
        adapted.delegate
      case delegate: CanTriggerReplay => new ScalaToJavaBySlicesSourceProviderAdapterWithCanTriggerReplay(delegate)
      case _                          => new ScalaToJavaBySlicesSourceProviderAdapter(delegate)
    }
}

/**
 * INTERNAL API: Adapter from scaladsl.SourceProvider with BySlicesSourceProvider to javadsl.SourceProvider with BySlicesSourceProvider
 */
@InternalApi
private[projection] sealed class ScalaToJavaBySlicesSourceProviderAdapter[Offset, Envelope] private[internal] (
    val delegate: scaladsl.SourceProvider[Offset, Envelope] with BySlicesSourceProvider)
    extends javadsl.SourceProvider[Offset, Envelope]
    with BySlicesSourceProvider
    with EventTimestampQuery
    with LoadEventQuery {
  override def source(
      offset: Supplier[CompletionStage[Optional[Offset]]]): CompletionStage[JSource[Envelope, NotUsed]] =
    delegate
      .source(() => offset.get().asScala.map(_.toScala)(ExecutionContext.parasitic))
      .map(_.asJava)(ExecutionContext.parasitic)
      .asJava

  override def extractOffset(envelope: Envelope): Offset = delegate.extractOffset(envelope)

  override def extractCreationTime(envelope: Envelope): Long = delegate.extractCreationTime(envelope)

  def minSlice: Int = delegate.minSlice

  def maxSlice: Int = delegate.maxSlice

  override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
    delegate match {
      case etq: ScalaEventTimestampQuery =>
        etq.timestampOf(persistenceId, sequenceNr).map(_.toJava)(ExecutionContext.parasitic).asJava
      case _ =>
        throw new IllegalStateException(
          s"timestampOf was called but delegate of type [${delegate.getClass}] does not implement akka.persistence.query.typed.scaladsl.EventTimestampQuery")
    }

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]] =
    delegate match {
      case etq: ScalaLoadEventQuery =>
        etq.loadEnvelope[Event](persistenceId, sequenceNr).asJava
      case _ =>
        throw new IllegalStateException(
          s"loadEnvelope was called but delegate of type [${delegate.getClass}] does not implement akka.persistence.query.typed.scaladsl.LoadEventQuery")
    }

}

/**
 * INTERNAL API: Adapter from scaladsl.SourceProvider with BySlicesSourceProvider to javadsl.SourceProvider with BySlicesSourceProvider
 */
@InternalApi
private[projection] final class ScalaToJavaBySlicesSourceProviderAdapterWithCanTriggerReplay[Offset, Envelope] private[internal] (
    delegate: scaladsl.SourceProvider[Offset, Envelope] with BySlicesSourceProvider with CanTriggerReplay)
    extends ScalaToJavaBySlicesSourceProviderAdapter[Offset, Envelope](delegate)
    with CanTriggerReplay {

  override private[akka] def triggerReplay(persistenceId: String, fromSeqNr: Long, triggeredBySeqNr: Long): Unit =
    delegate.triggerReplay(persistenceId, fromSeqNr, triggeredBySeqNr)
}
