/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.function.Supplier
import java.util.function.{ Function => JFunction }

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

import akka.NotUsed
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.projection.BySlicesSourceProvider
import akka.projection.internal.SourceProviderAdapter
import akka.projection.javadsl
import akka.projection.javadsl.CustomStartOffsetSourceProvider
import akka.projection.scaladsl
import akka.stream.scaladsl.Source

@InternalApi private[projection] object BySliceSourceProviderAdapter {
  def apply[Offset, Envelope](
      sourceProvider: javadsl.SourceProvider[Offset, Envelope]): scaladsl.SourceProvider[Offset, Envelope] =
    sourceProvider match {
      case _: BySlicesSourceProvider =>
        new BySliceSourceProviderAdapter[Offset, Envelope](
          loadFromOffsetStore = true,
          adjustStartOffset = offset => CompletableFuture.completedFuture(offset),
          sourceProvider)
      case c: CustomStartOffsetSourceProvider[Offset, Envelope] @unchecked =>
        c.delegate match {
          case _: BySlicesSourceProvider =>
            new BySliceSourceProviderAdapter[Offset, Envelope](
              loadFromOffsetStore = true,
              adjustStartOffset = offset => CompletableFuture.completedFuture(offset),
              c.delegate)
          case _ =>
            new SourceProviderAdapter(sourceProvider)
        }
      case _ =>
        new SourceProviderAdapter(sourceProvider)
    }

}

/**
 * INTERNAL API: Adapter from javadsl.SourceProvider to scaladsl.SourceProvider
 */
@InternalApi private[projection] class BySliceSourceProviderAdapter[Offset, Envelope](
    loadFromOffsetStore: Boolean,
    adjustStartOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]],
    delegate: javadsl.SourceProvider[Offset, Envelope])
    extends scaladsl.SourceProvider[Offset, Envelope]
    with BySlicesSourceProvider
    with EventTimestampQuery
    with LoadEventQuery {

  def source(fromOffsetStore: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]] = {
    val startOffsetFn: Supplier[CompletionStage[Optional[Offset]]] = {
      if (loadFromOffsetStore) { () =>
        fromOffsetStore().toJava.thenCompose { offset =>
          adjustStartOffset(offset.asJava)
        }
      } else { () =>
        adjustStartOffset(Optional.empty[Offset])
      }
    }

    delegate.source(startOffsetFn).thenApply(_.asScala).toScala
  }

  def extractOffset(envelope: Envelope): Offset = delegate.extractOffset(envelope)

  def extractCreationTime(envelope: Envelope): Long = delegate.extractCreationTime(envelope)

  override def minSlice: Int =
    delegate.asInstanceOf[BySlicesSourceProvider].minSlice

  override def maxSlice: Int =
    delegate.asInstanceOf[BySlicesSourceProvider].maxSlice

  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] =
    delegate match {
      case timestampQuery: akka.persistence.query.typed.javadsl.EventTimestampQuery =>
        timestampQuery.timestampOf(persistenceId, sequenceNr).toScala.map(_.asScala)(ExecutionContexts.parasitic)
      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"Expected SourceProvider [${delegate.getClass.getName}] to implement " +
            s"EventTimestampQuery when TimestampOffset is used."))
    }

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] =
    delegate match {
      case timestampQuery: akka.persistence.query.typed.javadsl.LoadEventQuery =>
        timestampQuery.loadEnvelope[Event](persistenceId, sequenceNr).toScala
      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"Expected SourceProvider [${delegate.getClass.getName}] to implement " +
            s"EventTimestampQuery when LoadEventQuery is used."))
    }
}
