/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier
import scala.concurrent.Future
import akka.NotUsed
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.projection.javadsl
import akka.projection.scaladsl
import akka.stream.scaladsl.Source

import scala.compat.java8.FutureConverters._

import scala.compat.java8.OptionConverters._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.projection.BySlicesSourceProvider

/**
 * INTERNAL API: Adapter from javadsl.SourceProvider to scaladsl.SourceProvider
 */
@InternalApi private[projection] class BySliceSourceProviderAdapter[Offset, Envelope](
    delegate: javadsl.SourceProvider[Offset, Envelope])
    extends scaladsl.SourceProvider[Offset, Envelope]
    with BySlicesSourceProvider
    with EventTimestampQuery
    with LoadEventQuery {

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
