/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.eventsourced.javadsl

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.function.Supplier
import java.util.function.{ Function => JFunction }

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.japi.Pair
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.javadsl.EventsByTagQuery
import akka.persistence.query.javadsl.ReadJournal
import akka.persistence.query.typed.javadsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.javadsl.EventTimestampQuery
import akka.persistence.query.typed.javadsl.EventsBySliceQuery
import akka.persistence.query.typed.javadsl.EventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.javadsl.LatestEventTimestampQuery
import akka.persistence.query.typed.javadsl.LoadEventQuery
import akka.projection.BySlicesSourceProvider
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider.LoadEventsByPersistenceIdSourceProvider
import akka.projection.internal.CanTriggerReplay
import akka.projection.internal.BacklogStatusSourceProvider
import akka.projection.javadsl
import akka.projection.javadsl.SourceProvider
import akka.stream.javadsl.Source

object EventSourcedProvider {

  def eventsByTag[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {
    val eventsByTagQuery =
      PersistenceQuery(system).getReadJournalFor(classOf[EventsByTagQuery], readJournalPluginId)
    eventsByTag(system, eventsByTagQuery, tag)
  }

  def eventsByTag[Event](
      system: ActorSystem[_],
      eventsByTagQuery: EventsByTagQuery,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {
    new EventsByTagSourceProvider(system, eventsByTagQuery, tag)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  @nowarn("msg=never used") // system
  private class EventsByTagSourceProvider[Event](
      system: ActorSystem[_],
      eventsByTagQuery: EventsByTagQuery,
      tag: String)
      extends javadsl.SourceProvider[Offset, EventEnvelope[Event]] {

    override def source(offsetAsync: Supplier[CompletionStage[Optional[Offset]]])
        : CompletionStage[Source[EventEnvelope[Event], NotUsed]] = {
      offsetAsync.get().thenApply { storedOffset =>
        eventsByTagQuery
          .eventsByTag(tag, storedOffset.orElse(NoOffset))
          .map(env => EventEnvelope(env))
      }
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
    eventsBySlices(system, eventsBySlicesQuery, entityType, minSlice, maxSlice)
  }

  /**
   * By default, the `SourceProvider` uses the stored offset when starting the Projection. This offset can be adjusted
   * by defining the `adjustStartOffset` function, which is a function from loaded offset (if any) to the
   * adjusted offset that will be used to by the `eventsBySlicesQuery`.
   */
  def eventsBySlices[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      adjustStartOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).getReadJournalFor(classOf[EventsBySliceQuery], readJournalPluginId)
    eventsBySlices(system, eventsBySlicesQuery, entityType, minSlice, maxSlice, adjustStartOffset)
  }

  def eventsBySlices[Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int): SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] =
    eventsBySlices(
      system,
      eventsBySlicesQuery,
      entityType,
      minSlice,
      maxSlice,
      (offset: Optional[Offset]) => CompletableFuture.completedFuture(offset))

  /**
   * By default, the `SourceProvider` uses the stored offset when starting the Projection. This offset can be adjusted
   * by defining the `adjustStartOffset` function, which is a function from loaded offset (if any) to the
   * adjusted offset that will be used to by the `eventsBySlicesQuery`.
   */
  def eventsBySlices[Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      adjustStartOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    eventsBySlicesQuery match {
      case query: EventsBySliceQuery with CanTriggerReplay =>
        new EventsBySlicesSourceProvider[Event](
          system,
          eventsBySlicesQuery,
          entityType,
          minSlice,
          maxSlice,
          adjustStartOffset) with CanTriggerReplay {

          private[akka] override def triggerReplay(
              persistenceId: String,
              fromSeqNr: Long,
              triggeredBySeqNr: Long): Unit =
            query.triggerReplay(persistenceId, fromSeqNr, triggeredBySeqNr)

        }
      case _ =>
        new EventsBySlicesSourceProvider(system, eventsBySlicesQuery, entityType, minSlice, maxSlice, adjustStartOffset)
    }
  }

  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: java.util.function.Function[Snapshot, Event])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).getReadJournalFor(classOf[EventsBySliceStartingFromSnapshotsQuery], readJournalPluginId)
    eventsBySlicesStartingFromSnapshots(system, eventsBySlicesQuery, entityType, minSlice, maxSlice, transformSnapshot)
  }

  /**
   * By default, the `SourceProvider` uses the stored offset when starting the Projection. This offset can be adjusted
   * by defining the `adjustStartOffset` function, which is a function from loaded offset (if any) to the
   * adjusted offset that will be used to by the `eventsBySlicesQuery`.
   */
  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: java.util.function.Function[Snapshot, Event],
      adjustStartOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).getReadJournalFor(classOf[EventsBySliceStartingFromSnapshotsQuery], readJournalPluginId)
    eventsBySlicesStartingFromSnapshots(
      system,
      eventsBySlicesQuery,
      entityType,
      minSlice,
      maxSlice,
      transformSnapshot,
      adjustStartOffset)
  }

  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceStartingFromSnapshotsQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: java.util.function.Function[Snapshot, Event])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] =
    eventsBySlicesStartingFromSnapshots(
      system,
      eventsBySlicesQuery,
      entityType,
      minSlice,
      maxSlice,
      transformSnapshot,
      (offset: Optional[Offset]) => CompletableFuture.completedFuture(offset))

  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceStartingFromSnapshotsQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: java.util.function.Function[Snapshot, Event],
      adjustStartOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    eventsBySlicesQuery match {
      case query: EventsBySliceQuery with CanTriggerReplay =>
        new EventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
          system,
          eventsBySlicesQuery,
          entityType,
          minSlice,
          maxSlice,
          transformSnapshot,
          adjustStartOffset) with CanTriggerReplay {

          private[akka] override def triggerReplay(
              persistenceId: String,
              fromSeqNr: Long,
              triggeredBySeqNr: Long): Unit =
            query.triggerReplay(persistenceId, fromSeqNr, triggeredBySeqNr)

        }
      case _ =>
        new EventsBySlicesStartingFromSnapshotsSourceProvider(
          system,
          eventsBySlicesQuery,
          entityType,
          minSlice,
          maxSlice,
          transformSnapshot,
          adjustStartOffset)
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
  @nowarn("msg=never used") // system
  private class EventsBySlicesSourceProvider[Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      adjustStartOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]])
      extends SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]]
      with BySlicesSourceProvider
      with EventTimestampQuerySourceProvider
      with LoadEventQuerySourceProvider
      with LoadEventsByPersistenceIdSourceProvider[Event]
      with BacklogStatusSourceProvider {

    override def readJournal: ReadJournal = eventsBySlicesQuery

    override def source(offsetAsync: Supplier[CompletionStage[Optional[Offset]]])
        : CompletionStage[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      offsetAsync.get().thenCompose { storedOffset =>
        adjustStartOffset(storedOffset).thenApply { startOffset =>
          eventsBySlicesQuery
            .eventsBySlices(entityType, minSlice, maxSlice, startOffset.orElse(NoOffset))
        }
      }
    }

    override def extractOffset(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Long =
      envelope.timestamp

    /**
     * INTERNAL API
     */
    @InternalApi override private[akka] def currentEventsByPersistenceId(
        persistenceId: String,
        fromSequenceNr: Long,
        toSequenceNr: Long)
        : Option[akka.stream.scaladsl.Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      eventsBySlicesQuery match {
        case q: CurrentEventsByPersistenceIdTypedQuery =>
          Some(q.currentEventsByPersistenceIdTyped[Event](persistenceId, fromSequenceNr, toSequenceNr).asScala)
        case _ => None // not supported by this query
      }
    }

    /** INTERNAL API */
    @InternalApi
    override private[akka] def supportsLatestEventTimestamp: Boolean =
      eventsBySlicesQuery.isInstanceOf[LatestEventTimestampQuery]

    /** INTERNAL API */
    @InternalApi
    override private[akka] def latestEventTimestamp(): Future[Option[Instant]] = {
      eventsBySlicesQuery match {
        case query: LatestEventTimestampQuery =>
          query.latestEventTimestamp(entityType, minSlice, maxSlice).asScala.map(_.toScala)(ExecutionContext.parasitic)
        case _ => Future.successful(None)
      }
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  @nowarn("msg=never used") // system
  private class EventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceStartingFromSnapshotsQuery,
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      transformSnapshot: java.util.function.Function[Snapshot, Event],
      adjustStartOffset: JFunction[Optional[Offset], CompletionStage[Optional[Offset]]])
      extends SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]]
      with BySlicesSourceProvider
      with EventTimestampQuerySourceProvider
      with LoadEventQuerySourceProvider
      with LoadEventsByPersistenceIdSourceProvider[Event]
      with BacklogStatusSourceProvider {

    override def readJournal: ReadJournal = eventsBySlicesQuery

    override def source(offsetAsync: Supplier[CompletionStage[Optional[Offset]]])
        : CompletionStage[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      offsetAsync.get().thenCompose { storedOffset =>
        adjustStartOffset(storedOffset).thenApply { startOffset =>
          eventsBySlicesQuery
            .eventsBySlicesStartingFromSnapshots(
              entityType,
              minSlice,
              maxSlice,
              startOffset.orElse(NoOffset),
              transformSnapshot)
        }
      }
    }

    override def extractOffset(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Long =
      envelope.timestamp

    /**
     * INTERNAL API
     */
    @InternalApi override private[akka] def currentEventsByPersistenceId(
        persistenceId: String,
        fromSequenceNr: Long,
        toSequenceNr: Long)
        : Option[akka.stream.scaladsl.Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      eventsBySlicesQuery match {
        case q: CurrentEventsByPersistenceIdTypedQuery =>
          Some(q.currentEventsByPersistenceIdTyped[Event](persistenceId, fromSequenceNr, toSequenceNr).asScala)
        case _ => None // not supported by this query
      }
    }

    /** INTERNAL API */
    @InternalApi
    override private[akka] def supportsLatestEventTimestamp: Boolean =
      eventsBySlicesQuery.isInstanceOf[LatestEventTimestampQuery]

    /** INTERNAL API */
    @InternalApi
    override private[akka] def latestEventTimestamp(): Future[Option[Instant]] = {
      eventsBySlicesQuery match {
        case query: LatestEventTimestampQuery =>
          query.latestEventTimestamp(entityType, minSlice, maxSlice).asScala.map(_.toScala)(ExecutionContext.parasitic)
        case _ => Future.successful(None)
      }
    }
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
