/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.eventsourced.javadsl

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.function.Supplier
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.japi.Pair
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.javadsl.EventsByTagQuery
import akka.persistence.query.typed.javadsl.EventTimestampQuery
import akka.persistence.query.typed.javadsl.EventsBySliceQuery
import akka.persistence.query.typed.javadsl.LoadEventQuery
import akka.projection.BySlicesSourceProvider
import akka.projection.eventsourced.EventEnvelope
import akka.projection.javadsl
import akka.projection.javadsl.SourceProvider
import akka.stream.javadsl.Source

@ApiMayChange
object EventSourcedProvider {

  def eventsByTag[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {

    val eventsByTagQuery =
      PersistenceQuery(system).getReadJournalFor(classOf[EventsByTagQuery], readJournalPluginId)

    new EventsByTagSourceProvider(system, eventsByTagQuery, tag)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private class EventsByTagSourceProvider[Event](
      system: ActorSystem[_],
      eventsByTagQuery: EventsByTagQuery,
      tag: String)
      extends javadsl.SourceProvider[Offset, EventEnvelope[Event]] {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def source(offsetAsync: Supplier[CompletionStage[Optional[Offset]]])
        : CompletionStage[Source[EventEnvelope[Event], NotUsed]] = {
      val source: Future[Source[EventEnvelope[Event], NotUsed]] = offsetAsync.get().toScala.map { offsetOpt =>
        eventsByTagQuery
          .eventsByTag(tag, offsetOpt.orElse(NoOffset))
          .map(env => EventEnvelope(env))
      }
      source.toJava
    }

    override def extractOffset(envelope: EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: EventEnvelope[Event]): Long = envelope.timestamp
  }

  def eventsBySlices[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int): SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {

    val eventsBySlicesQuery =
      PersistenceQuery(system).getReadJournalFor(classOf[EventsBySliceQuery], readJournalPluginId)

    if (!eventsBySlicesQuery.isInstanceOf[EventTimestampQuery])
      throw new IllegalArgumentException(
        s"[${eventsBySlicesQuery.getClass.getName}] with readJournalPluginId " +
        s"[$readJournalPluginId] must implement [${classOf[EventTimestampQuery].getName}]")

    if (!eventsBySlicesQuery.isInstanceOf[LoadEventQuery])
      throw new IllegalArgumentException(
        s"[${eventsBySlicesQuery.getClass.getName}] with readJournalPluginId " +
        s"[$readJournalPluginId] must implement [${classOf[LoadEventQuery].getName}]")

    eventsBySlices[Event](system, eventsBySlicesQuery, entityType, minSlice, maxSlice)
  }

  def eventsBySlices[Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int): SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {

    if (!eventsBySlicesQuery.isInstanceOf[EventTimestampQuery])
      throw new IllegalArgumentException(
        s"[${eventsBySlicesQuery.getClass.getName}] must implement [${classOf[EventTimestampQuery].getName}]")

    if (!eventsBySlicesQuery.isInstanceOf[LoadEventQuery])
      throw new IllegalArgumentException(
        s"[${eventsBySlicesQuery.getClass.getName}] must implement [${classOf[LoadEventQuery].getName}]")

    new EventsBySlicesSourceProvider(eventsBySlicesQuery, entityType, minSlice, maxSlice, system)
  }

  def sliceForPersistenceId(system: ActorSystem[_], readJournalPluginId: String, persistenceId: String): Int =
    PersistenceQuery(system)
      .getReadJournalFor(classOf[EventsBySliceQuery], readJournalPluginId)
      .sliceForPersistenceId(persistenceId)

  def sliceRanges(
      system: ActorSystem[_],
      readJournalPluginId: String,
      numberOfRanges: Int): java.util.List[Pair[Integer, Integer]] =
    PersistenceQuery(system)
      .getReadJournalFor(classOf[EventsBySliceQuery], readJournalPluginId)
      .sliceRanges(numberOfRanges)

  /**
   * INTERNAL API
   */
  @InternalApi
  private class EventsBySlicesSourceProvider[Event](
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      system: ActorSystem[_])
      extends SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]]
      with BySlicesSourceProvider
      with EventTimestampQuery
      with LoadEventQuery {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def source(offsetAsync: Supplier[CompletionStage[Optional[Offset]]])
        : CompletionStage[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      val source: Future[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] =
        offsetAsync.get().toScala.map { offsetOpt =>
          eventsBySlicesQuery
            .eventsBySlices(entityType, minSlice, maxSlice, offsetOpt.orElse(NoOffset))
        }
      source.toJava
    }

    override def extractOffset(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Long =
      envelope.timestamp

    override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
      eventsBySlicesQuery match {
        case timestampQuery: EventTimestampQuery =>
          timestampQuery.timestampOf(persistenceId, sequenceNr)
        case _ =>
          val failed = new CompletableFuture[Optional[Instant]]
          failed.completeExceptionally(
            new IllegalStateException(
              s"[${eventsBySlicesQuery.getClass.getName}] must implement [${classOf[EventTimestampQuery].getName}]"))
          failed.toCompletableFuture
      }

    override def loadEnvelope[Evt](
        persistenceId: String,
        sequenceNr: Long): CompletionStage[akka.persistence.query.typed.EventEnvelope[Evt]] =
      eventsBySlicesQuery match {
        case laodEventQuery: LoadEventQuery =>
          laodEventQuery.loadEnvelope(persistenceId, sequenceNr)
        case _ =>
          val failed = new CompletableFuture[akka.persistence.query.typed.EventEnvelope[Evt]]
          failed.completeExceptionally(
            new IllegalStateException(
              s"[${eventsBySlicesQuery.getClass.getName}] must implement [${classOf[LoadEventQuery].getName}]"))
          failed.toCompletableFuture
      }
  }
}
