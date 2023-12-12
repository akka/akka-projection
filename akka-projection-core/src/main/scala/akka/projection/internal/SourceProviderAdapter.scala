/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.dispatch.ExecutionContexts
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.javadsl.EventTimestampQuery
import akka.persistence.query.typed.javadsl.LoadEventQuery
import akka.persistence.query.typed.scaladsl.{ EventTimestampQuery => ScalaEventTimestampQuery }
import akka.persistence.query.typed.scaladsl.{ LoadEventQuery => ScalaLoadEventQuery }
import akka.projection.BySlicesSourceProvider
import akka.projection.javadsl
import akka.projection.scaladsl
import akka.stream.javadsl.{ Source => JSource }
import akka.stream.scaladsl.Source

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

/**
 * INTERNAL API: Adapter from javadsl.SourceProvider to scaladsl.SourceProvider
 */
@InternalStableApi private[projection] class SourceProviderAdapter[Offset, Envelope](
    delegate: javadsl.SourceProvider[Offset, Envelope])
    extends scaladsl.SourceProvider[Offset, Envelope] {

  def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]] = {
    // the parasitic context is used to convert the Optional to Option and a java streams Source to a scala Source,	
    // it _should_ not be used for the blocking operation of getting offsets themselves	
    val ec = akka.dispatch.ExecutionContexts.parasitic
    val offsetAdapter = new Supplier[CompletionStage[Optional[Offset]]] {
      override def get(): CompletionStage[Optional[Offset]] = offset().map(_.asJava)(ec).toJava
    }
    delegate.source(offsetAdapter).toScala.map(_.asScala)(ec)
  }

  def extractOffset(envelope: Envelope): Offset = delegate.extractOffset(envelope)

  def extractCreationTime(envelope: Envelope): Long = delegate.extractCreationTime(envelope)
}

/**
 * INTERNAL API: Adapter from scaladsl.SourceProvider with BySlicesSourceProvider to javadsl.SourceProvider with BySlicesSourceProvider
 */
@InternalApi private[projection] class ScalaBySlicesSourceProviderAdapter[Offset, Envelope](
    delegate: scaladsl.SourceProvider[Offset, Envelope] with BySlicesSourceProvider)
    extends javadsl.SourceProvider[Offset, Envelope]
    with BySlicesSourceProvider
    with EventTimestampQuery
    with LoadEventQuery
    with CanTriggerReplay {
  override def source(
      offset: Supplier[CompletionStage[Optional[Offset]]]): CompletionStage[JSource[Envelope, NotUsed]] =
    delegate
      .source(() => offset.get().toScala.map(_.asScala)(ExecutionContexts.parasitic))
      .map(_.asJava)(ExecutionContexts.parasitic)
      .toJava

  override def extractOffset(envelope: Envelope): Offset = delegate.extractOffset(envelope)

  override def extractCreationTime(envelope: Envelope): Long = delegate.extractCreationTime(envelope)

  def minSlice: Int = delegate.minSlice

  def maxSlice: Int = delegate.maxSlice

  override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
    delegate match {
      case etq: ScalaEventTimestampQuery =>
        etq.timestampOf(persistenceId, sequenceNr).map(_.asJava)(ExecutionContexts.parasitic).toJava
      case _ =>
        throw new IllegalStateException(
          s"timestampOf was called but delegate of type [${delegate.getClass}] does not implement akka.persistence.query.typed.scaladsl.EventTimestampQuery")
    }

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]] =
    delegate match {
      case etq: ScalaLoadEventQuery =>
        etq.loadEnvelope[Event](persistenceId, sequenceNr).toJava
      case _ =>
        throw new IllegalStateException(
          s"loadEnvelope was called but delegate of type [${delegate.getClass}] does not implement akka.persistence.query.typed.scaladsl.LoadEventQuery")
    }

  override private[akka] def triggerReplay(persistenceId: String, fromSeqNr: Long): Unit = {
    delegate match {
      case ctr: CanTriggerReplay =>
        ctr.triggerReplay(persistenceId, fromSeqNr)
      case _ =>
        throw new IllegalStateException(
          s"triggerReplay was called but delegate of type [${delegate.getClass}] does not implement CanTriggerReplay")
    }
  }
}
