/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.projection.BySlicesSourceProvider
import akka.projection.javadsl
import akka.projection.scaladsl
import akka.stream.scaladsl.Source
import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._
import scala.concurrent.{ ExecutionContext, Future }

import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery

@InternalApi private[projection] object JavaToScalaBySliceSourceProviderAdapter {
  def apply[Offset, Envelope](
      delegate: javadsl.SourceProvider[Offset, Envelope]): scaladsl.SourceProvider[Offset, Envelope] =
    delegate match {
      case adapted: ScalaToJavaBySlicesSourceProviderAdapter[_, _] =>
        // just unwrap rather than wrapping further
        adapted.delegate
      case delegate: BySlicesSourceProvider with CanTriggerReplay =>
        new JavaToScalaBySliceSourceProviderAdapterWithCanTriggerReplay(delegate)
      case _: BySlicesSourceProvider => new JavaToScalaBySliceSourceProviderAdapter(delegate)
      case _                         => new JavaToScalaSourceProviderAdapter(delegate)
    }
}

/**
 * INTERNAL API: Adapter from javadsl.SourceProvider to scaladsl.SourceProvider
 */
private[projection] class JavaToScalaSourceProviderAdapter[Offset, Envelope](
    delegate: javadsl.SourceProvider[Offset, Envelope])
    extends scaladsl.SourceProvider[Offset, Envelope] {

  def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]] = {
    // the parasitic context is used to convert the Optional to Option and a java streams Source to a scala Source,
    // it _should_ not be used for the blocking operation of getting offsets themselves
    val ec = scala.concurrent.ExecutionContext.parasitic
    val offsetAdapter = new Supplier[CompletionStage[Optional[Offset]]] {
      override def get(): CompletionStage[Optional[Offset]] = offset().map(_.toJava)(ec).asJava
    }
    delegate.source(offsetAdapter).asScala.map(_.asScala)(ec)
  }

  def extractOffset(envelope: Envelope): Offset = delegate.extractOffset(envelope)

  def extractCreationTime(envelope: Envelope): Long = delegate.extractCreationTime(envelope)
}

/**
 * INTERNAL API: Adapter from javadsl.SourceProvider to scaladsl.SourceProvider
 */
@InternalApi private[projection] sealed class JavaToScalaBySliceSourceProviderAdapter[Offset, Envelope] private[internal] (
    val delegate: javadsl.SourceProvider[Offset, Envelope])
    extends scaladsl.SourceProvider[Offset, Envelope]
    with BySlicesSourceProvider
    with BacklogStatusSourceProvider
    with EventTimestampQuery
    with LoadEventQuery
    with CurrentEventsByPersistenceIdTypedQuery {

  def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]] = {
    // the parasitic context is used to convert the Optional to Option and a java streams Source to a scala Source,
    // it _should_ not be used for the blocking operation of getting offsets themselves
    val ec = scala.concurrent.ExecutionContext.parasitic
    val offsetAdapter = new Supplier[CompletionStage[Optional[Offset]]] {
      override def get(): CompletionStage[Optional[Offset]] = offset().map(_.toJava)(ec).asJava
    }
    delegate.source(offsetAdapter).asScala.map(_.asScala)(ec)
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
        timestampQuery.timestampOf(persistenceId, sequenceNr).asScala.map(_.toScala)(ExecutionContext.parasitic)
      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"Expected SourceProvider [${delegate.getClass.getName}] to implement " +
            s"EventTimestampQuery when TimestampOffset is used."))
    }

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] =
    delegate match {
      case timestampQuery: akka.persistence.query.typed.javadsl.LoadEventQuery =>
        timestampQuery.loadEnvelope[Event](persistenceId, sequenceNr).asScala
      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"Expected SourceProvider [${delegate.getClass.getName}] to implement " +
            s"EventTimestampQuery when LoadEventQuery is used."))
    }

  override private[akka] def supportsLatestEventTimestamp: Boolean =
    delegate match {
      case sourceProvider: BacklogStatusSourceProvider =>
        sourceProvider.supportsLatestEventTimestamp
      case _ => false
    }

  override private[akka] def latestEventTimestamp(): Future[Option[Instant]] =
    delegate match {
      case sourceProvider: BacklogStatusSourceProvider =>
        sourceProvider.latestEventTimestamp()
      case _ => Future.successful(None)
    }

  override def currentEventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] =
    delegate match {
      case eventsQuery: akka.persistence.query.typed.javadsl.CurrentEventsByPersistenceIdTypedQuery =>
        eventsQuery.currentEventsByPersistenceIdTyped[Event](persistenceId, fromSequenceNr, toSequenceNr).asScala
      case _ =>
        Source.failed(
          new IllegalArgumentException(
            s"Expected SourceProvider [${delegate.getClass.getName}] to implement " +
            s"CurrentEventsByPersistenceIdTypedQuery when currentEventsByPersistenceIdTyped is used."))
    }

}

/**
 * INTERNAL API: Adapter from javadsl.SourceProvider to scaladsl.SourceProvider that also implements
 * CanTriggerReplay
 */
@InternalApi
private[projection] final class JavaToScalaBySliceSourceProviderAdapterWithCanTriggerReplay[Offset, Envelope] private[internal] (
    delegate: javadsl.SourceProvider[Offset, Envelope] with CanTriggerReplay)
    extends JavaToScalaBySliceSourceProviderAdapter[Offset, Envelope](delegate)
    with CanTriggerReplay {
  override private[akka] def triggerReplay(persistenceId: String, fromSeqNr: Long, triggeredBySeqNr: Long): Unit =
    delegate.triggerReplay(persistenceId, fromSeqNr, triggeredBySeqNr)
}
