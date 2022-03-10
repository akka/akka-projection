/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.eventsourced.scaladsl

import java.time.Instant

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.projection.BySlicesSourceProvider
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

@ApiMayChange
object EventSourcedProvider {

  def eventsByTag[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {

    val eventsByTagQuery =
      PersistenceQuery(system).readJournalFor[EventsByTagQuery](readJournalPluginId)

    new EventsByTagSourceProvider(eventsByTagQuery, tag, system)
  }

  private class EventsByTagSourceProvider[Event](
      eventsByTagQuery: EventsByTagQuery,
      tag: String,
      system: ActorSystem[_])
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

    new EventsBySlicesSourceProvider(eventsBySlicesQuery, entityType, minSlice, maxSlice, system)
  }

  def sliceForPersistenceId(system: ActorSystem[_], readJournalPluginId: String, persistenceId: String): Int =
    PersistenceQuery(system)
      .readJournalFor[EventsBySliceQuery](readJournalPluginId)
      .sliceForPersistenceId(persistenceId)

  def sliceRanges(system: ActorSystem[_], readJournalPluginId: String, numberOfRanges: Int): immutable.Seq[Range] =
    PersistenceQuery(system).readJournalFor[EventsBySliceQuery](readJournalPluginId).sliceRanges(numberOfRanges)

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

    override def source(offset: () => Future[Option[Offset]])
        : Future[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] =
      offset().map { offsetOpt =>
        val offset = offsetOpt.getOrElse(NoOffset)
        eventsBySlicesQuery.eventsBySlices(entityType, minSlice, maxSlice, offset)
      }

    override def extractOffset(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Long =
      envelope.timestamp

    override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] =
      eventsBySlicesQuery match {
        case timestampQuery: EventTimestampQuery =>
          timestampQuery.timestampOf(persistenceId, sequenceNr)
        case _ =>
          Future.failed(
            new IllegalStateException(
              s"[${eventsBySlicesQuery.getClass.getName}] must implement [${classOf[EventTimestampQuery].getName}]"))
      }

    override def loadEnvelope[Evt](
        persistenceId: String,
        sequenceNr: Long): Future[akka.persistence.query.typed.EventEnvelope[Evt]] =
      eventsBySlicesQuery match {
        case laodEventQuery: LoadEventQuery =>
          laodEventQuery.loadEnvelope(persistenceId, sequenceNr)
        case _ =>
          Future.failed(
            new IllegalStateException(
              s"[${eventsBySlicesQuery.getClass.getName}] must implement [${classOf[LoadEventQuery].getName}]"))
      }
  }

}
