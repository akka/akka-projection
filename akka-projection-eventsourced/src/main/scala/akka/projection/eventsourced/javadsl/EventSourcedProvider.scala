/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
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
import akka.annotation.InternalApi
import akka.japi.Pair
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.javadsl.EventsByTagQuery
import akka.persistence.query.javadsl.ReadJournal
import akka.persistence.query.typed.javadsl.EventTimestampQuery
import akka.persistence.query.typed.javadsl.EventsBySliceQuery
import akka.persistence.query.typed.javadsl.EventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.javadsl.LoadEventQuery
import akka.projection.BySlicesSourceProvider
import akka.projection.eventsourced.EventEnvelope
import akka.projection.internal.CanTriggerReplay
import akka.projection.javadsl.CustomStartOffsetSourceProvider
import akka.projection.javadsl.CustomStartOffsetSourceProviderImpl
import akka.stream.javadsl.Source

object EventSourcedProvider {

  def eventsByTag[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      tag: String): CustomStartOffsetSourceProvider[Offset, EventEnvelope[Event]] = {
    val eventsByTagQuery =
      PersistenceQuery(system).getReadJournalFor(classOf[EventsByTagQuery], readJournalPluginId)
    eventsByTag(system, eventsByTagQuery, tag)
  }

  def eventsByTag[Event](
      system: ActorSystem[_],
      eventsByTagQuery: EventsByTagQuery,
      tag: String): CustomStartOffsetSourceProvider[Offset, EventEnvelope[Event]] = {
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
      extends CustomStartOffsetSourceProviderImpl[Offset, EventEnvelope[Event]] {
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
      maxSlice: Int): CustomStartOffsetSourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).getReadJournalFor(classOf[EventsBySliceQuery], readJournalPluginId)
    eventsBySlices(system, eventsBySlicesQuery, entityType, minSlice, maxSlice)
  }

  def eventsBySlices[Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int): CustomStartOffsetSourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    eventsBySlicesQuery match {
      case query: EventsBySliceQuery with CanTriggerReplay =>
        new EventsBySlicesSourceProvider[Event](eventsBySlicesQuery, entityType, minSlice, maxSlice, system)
          with CanTriggerReplay {

          private[akka] override def triggerReplay(persistenceId: String, fromSeqNr: Long): Unit =
            query.triggerReplay(persistenceId, fromSeqNr)

        }
      case _ =>
        new EventsBySlicesSourceProvider(eventsBySlicesQuery, entityType, minSlice, maxSlice, system)
    }
  }

  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: java.util.function.Function[Snapshot, Event])
      : CustomStartOffsetSourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).getReadJournalFor(classOf[EventsBySliceStartingFromSnapshotsQuery], readJournalPluginId)
    eventsBySlicesStartingFromSnapshots(system, eventsBySlicesQuery, entityType, minSlice, maxSlice, transformSnapshot)
  }

  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceStartingFromSnapshotsQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: java.util.function.Function[Snapshot, Event])
      : CustomStartOffsetSourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    eventsBySlicesQuery match {
      case query: EventsBySliceQuery with CanTriggerReplay =>
        new EventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
          eventsBySlicesQuery,
          entityType,
          minSlice,
          maxSlice,
          transformSnapshot,
          system) with CanTriggerReplay {

          private[akka] override def triggerReplay(persistenceId: String, fromSeqNr: Long): Unit =
            query.triggerReplay(persistenceId, fromSeqNr)

        }
      case _ =>
        new EventsBySlicesStartingFromSnapshotsSourceProvider(
          eventsBySlicesQuery,
          entityType,
          minSlice,
          maxSlice,
          transformSnapshot,
          system)
    }
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
      extends CustomStartOffsetSourceProviderImpl[Offset, akka.persistence.query.typed.EventEnvelope[Event]]
      with BySlicesSourceProvider
      with EventTimestampQuerySourceProvider
      with LoadEventQuerySourceProvider {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def readJournal: ReadJournal = eventsBySlicesQuery

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

  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private class EventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
      eventsBySlicesQuery: EventsBySliceStartingFromSnapshotsQuery,
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      transformSnapshot: java.util.function.Function[Snapshot, Event],
      system: ActorSystem[_])
      extends CustomStartOffsetSourceProviderImpl[Offset, akka.persistence.query.typed.EventEnvelope[Event]]
      with BySlicesSourceProvider
      with EventTimestampQuerySourceProvider
      with LoadEventQuerySourceProvider {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def readJournal: ReadJournal = eventsBySlicesQuery

    override def source(offsetAsync: Supplier[CompletionStage[Optional[Offset]]])
        : CompletionStage[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      val source: Future[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] =
        offsetAsync.get().toScala.map { offsetOpt =>
          eventsBySlicesQuery
            .eventsBySlicesStartingFromSnapshots(
              entityType,
              minSlice,
              maxSlice,
              offsetOpt.orElse(NoOffset),
              transformSnapshot)
        }
      source.toJava
    }

    override def extractOffset(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Long =
      envelope.timestamp

  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private trait EventTimestampQuerySourceProvider extends EventTimestampQuery {
    def readJournal: ReadJournal

    override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
      readJournal match {
        case timestampQuery: EventTimestampQuery =>
          timestampQuery.timestampOf(persistenceId, sequenceNr)
        case _ =>
          val failed = new CompletableFuture[Optional[Instant]]
          failed.completeExceptionally(
            new IllegalStateException(
              s"[${readJournal.getClass.getName}] must implement [${classOf[EventTimestampQuery].getName}]"))
          failed.toCompletableFuture
      }
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private trait LoadEventQuerySourceProvider extends LoadEventQuery {
    def readJournal: ReadJournal

    override def loadEnvelope[Evt](
        persistenceId: String,
        sequenceNr: Long): CompletionStage[akka.persistence.query.typed.EventEnvelope[Evt]] =
      readJournal match {
        case laodEventQuery: LoadEventQuery =>
          laodEventQuery.loadEnvelope(persistenceId, sequenceNr)
        case _ =>
          val failed = new CompletableFuture[akka.persistence.query.typed.EventEnvelope[Evt]]
          failed.completeExceptionally(
            new IllegalStateException(
              s"[${readJournal.getClass.getName}] must implement [${classOf[LoadEventQuery].getName}]"))
          failed.toCompletableFuture
      }
  }
}
