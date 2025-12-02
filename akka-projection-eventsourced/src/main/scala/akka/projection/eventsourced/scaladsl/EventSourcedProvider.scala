/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.eventsourced.scaladsl

import java.time.Instant
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.QueryCorrelationId
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.scaladsl.LatestEventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.projection.BySlicesSourceProvider
import akka.projection.eventsourced.EventEnvelope
import akka.projection.internal.CanTriggerReplay
import akka.projection.internal.BacklogStatusSourceProvider
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

object EventSourcedProvider {

  def eventsByTag[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {
    val eventsByTagQuery =
      PersistenceQuery(system).readJournalFor[EventsByTagQuery](readJournalPluginId)
    eventsByTag(system, eventsByTagQuery, tag)
  }

  def eventsByTag[Event](
      system: ActorSystem[_],
      eventsByTagQuery: EventsByTagQuery,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {
    new EventsByTagSourceProvider(system, eventsByTagQuery, tag)
  }

  private class EventsByTagSourceProvider[Event](
      system: ActorSystem[_],
      eventsByTagQuery: EventsByTagQuery,
      tag: String)
      extends SourceProvider[Offset, EventEnvelope[Event]] {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def source(offset: () => Future[Option[Offset]]): Future[Source[EventEnvelope[Event], NotUsed]] =
      offset().map { offsetOpt =>
        val offset = offsetOpt.getOrElse(NoOffset)
        eventsByTagQuery
          .eventsByTag(tag, offset)
          .map(env => EventEnvelope(env))
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
      PersistenceQuery(system).readJournalFor[EventsBySliceQuery](readJournalPluginId)
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
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).readJournalFor[EventsBySliceQuery](readJournalPluginId)
    eventsBySlices(system, eventsBySlicesQuery, entityType, minSlice, maxSlice, adjustStartOffset)
  }

  def eventsBySlices[Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int): SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    eventsBySlices(system, eventsBySlicesQuery, entityType, minSlice, maxSlice, offset => Future.successful(offset))
  }

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
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
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
          override private[akka] def triggerReplay(
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
      transformSnapshot: Snapshot => Event)
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).readJournalFor[EventsBySliceStartingFromSnapshotsQuery](readJournalPluginId)
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
      transformSnapshot: Snapshot => Event,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).readJournalFor[EventsBySliceStartingFromSnapshotsQuery](readJournalPluginId)
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
      transformSnapshot: Snapshot => Event): SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] =
    eventsBySlicesStartingFromSnapshots(
      system,
      eventsBySlicesQuery,
      entityType,
      minSlice,
      maxSlice,
      transformSnapshot,
      offset => Future.successful(offset))

  /**
   * By default, the `SourceProvider` uses the stored offset when starting the Projection. This offset can be adjusted
   * by defining the `adjustStartOffset` function, which is a function from loaded offset (if any) to the
   * adjusted offset that will be used to by the `eventsBySlicesQuery`.
   */
  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceStartingFromSnapshotsQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: Snapshot => Event,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    eventsBySlicesQuery match {
      case query: EventsBySliceStartingFromSnapshotsQuery with CanTriggerReplay =>
        new EventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
          system,
          eventsBySlicesQuery,
          entityType,
          minSlice,
          maxSlice,
          transformSnapshot,
          adjustStartOffset) with CanTriggerReplay {
          override private[akka] def triggerReplay(
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
      .readJournalFor[EventsBySliceQuery](readJournalPluginId)
      .sliceForPersistenceId(persistenceId)

  def sliceRanges(system: ActorSystem[_], readJournalPluginId: String, numberOfRanges: Int): immutable.Seq[Range] =
    PersistenceQuery(system).readJournalFor[EventsBySliceQuery](readJournalPluginId).sliceRanges(numberOfRanges)

  private class EventsBySlicesSourceProvider[Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      extends SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]]
      with BySlicesSourceProvider
      with EventTimestampQuerySourceProvider
      with LoadEventQuerySourceProvider
      with LoadEventsByPersistenceIdSourceProvider[Event]
      with BacklogStatusSourceProvider {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def readJournal: ReadJournal = eventsBySlicesQuery

    override def source(offset: () => Future[Option[Offset]])
        : Future[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      val correlationId = QueryCorrelationId.get()
      for {
        storedOffset <- offset()
        startOffset <- adjustStartOffset(storedOffset)
      } yield {
        QueryCorrelationId.withCorrelationId(correlationId)(() =>
          eventsBySlicesQuery.eventsBySlices(entityType, minSlice, maxSlice, startOffset.getOrElse(NoOffset)))
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
        toSequenceNr: Long): Option[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      eventsBySlicesQuery match {
        case q: CurrentEventsByPersistenceIdTypedQuery =>
          Some(q.currentEventsByPersistenceIdTyped[Event](persistenceId, fromSequenceNr, toSequenceNr))
        case _ => None // not supported by this query
      }
    }

    override private[akka] def supportsLatestEventTimestamp: Boolean =
      eventsBySlicesQuery.isInstanceOf[LatestEventTimestampQuery]

    override private[akka] def latestEventTimestamp(): Future[Option[Instant]] =
      eventsBySlicesQuery match {
        case query: LatestEventTimestampQuery =>
          query.latestEventTimestamp(entityType, minSlice, maxSlice)
        case _ => Future.successful(None)
      }
  }

  private class EventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceStartingFromSnapshotsQuery,
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      transformSnapshot: Snapshot => Event,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      extends SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]]
      with BySlicesSourceProvider
      with EventTimestampQuerySourceProvider
      with LoadEventQuerySourceProvider
      with LoadEventsByPersistenceIdSourceProvider[Event]
      with BacklogStatusSourceProvider {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def readJournal: ReadJournal = eventsBySlicesQuery

    override def source(offset: () => Future[Option[Offset]])
        : Future[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      val correlationId = QueryCorrelationId.get()
      for {
        storedOffset <- offset()
        startOffset <- adjustStartOffset(storedOffset)
      } yield {
        QueryCorrelationId.withCorrelationId(correlationId)(
          () =>
            eventsBySlicesQuery.eventsBySlicesStartingFromSnapshots(
              entityType,
              minSlice,
              maxSlice,
              startOffset.getOrElse(NoOffset),
              transformSnapshot))
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
        toSequenceNr: Long): Option[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      eventsBySlicesQuery match {
        case q: CurrentEventsByPersistenceIdTypedQuery =>
          Some(q.currentEventsByPersistenceIdTyped[Event](persistenceId, fromSequenceNr, toSequenceNr))
        case _ => None // not supported by this query
      }
    }

    override private[akka] def supportsLatestEventTimestamp: Boolean =
      eventsBySlicesQuery.isInstanceOf[LatestEventTimestampQuery]

    override private[akka] def latestEventTimestamp(): Future[Option[Instant]] = {
      eventsBySlicesQuery match {
        case query: LatestEventTimestampQuery =>
          query.latestEventTimestamp(entityType, minSlice, maxSlice)
        case _ => Future.successful(None)
      }
    }

  }

  private trait EventTimestampQuerySourceProvider extends EventTimestampQuery {
    def readJournal: ReadJournal

    override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] =
      readJournal match {
        case timestampQuery: EventTimestampQuery =>
          timestampQuery.timestampOf(persistenceId, sequenceNr)
        case _ =>
          Future.failed(
            new IllegalStateException(
              s"[${readJournal.getClass.getName}] must implement [${classOf[EventTimestampQuery].getName}]"))
      }
  }

  private trait LoadEventQuerySourceProvider extends LoadEventQuery {
    def readJournal: ReadJournal

    override def loadEnvelope[Evt](
        persistenceId: String,
        sequenceNr: Long): Future[akka.persistence.query.typed.EventEnvelope[Evt]] =
      readJournal match {
        case loadEventQuery: LoadEventQuery =>
          loadEventQuery.loadEnvelope(persistenceId, sequenceNr)
        case _ =>
          Future.failed(
            new IllegalStateException(
              s"[${readJournal.getClass.getName}] must implement [${classOf[LoadEventQuery].getName}]"))
      }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] trait LoadEventsByPersistenceIdSourceProvider[Event] {

    /**
     * INTERNAL API
     */
    @InternalApi private[akka] def currentEventsByPersistenceId(
        persistenceId: String,
        fromSequenceNr: Long,
        toSequenceNr: Long): Option[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]]
  }

}
