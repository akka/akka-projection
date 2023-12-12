/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
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
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future
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
@InternalApi private[projection] sealed class JavaToScalaBySliceSourceProviderAdapter[Offset, Envelope] private[internal] (
    val delegate: javadsl.SourceProvider[Offset, Envelope])
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

/**
 * INTERNAL API: Adapter from javadsl.SourceProvider to scaladsl.SourceProvider that also implements
 * CanTriggerReplay
 */
@InternalApi
private[projection] final class JavaToScalaBySliceSourceProviderAdapterWithCanTriggerReplay[Offset, Envelope] private[internal] (
    delegate: javadsl.SourceProvider[Offset, Envelope] with CanTriggerReplay)
    extends JavaToScalaBySliceSourceProviderAdapter[Offset, Envelope](delegate)
    with CanTriggerReplay {
  override private[akka] def triggerReplay(persistenceId: String, fromSeqNr: Long): Unit =
    delegate.triggerReplay(persistenceId, fromSeqNr)
}
