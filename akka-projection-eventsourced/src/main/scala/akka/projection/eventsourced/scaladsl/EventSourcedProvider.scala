/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.eventsourced.scaladsl

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

object EventSourcedProvider {

  def eventsByTag[Event](
      systemProvider: ClassicActorSystemProvider,
      readJournalPluginId: String,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {

    val eventsByTagQuery =
      PersistenceQuery(systemProvider).readJournalFor[EventsByTagQuery](readJournalPluginId)

    new EventsByTagSourceProvider(eventsByTagQuery, tag, systemProvider)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private class EventsByTagSourceProvider[Event](
      eventsByTagQuery: EventsByTagQuery,
      tag: String,
      systemProvider: ClassicActorSystemProvider)
      extends SourceProvider[Offset, EventEnvelope[Event]] {
    implicit val dispatcher: ExecutionContext = systemProvider.classicSystem.dispatcher

    override def source(offset: () => Future[Option[Offset]]): Future[Source[EventEnvelope[Event], NotUsed]] =
      offset().map { offsetOpt =>
        val offset = offsetOpt.getOrElse(NoOffset)
        eventsByTagQuery
          .eventsByTag(tag, offset)
          .map(env => EventEnvelope(env))
      }

    override def extractOffset(envelope: EventEnvelope[Event]): Offset = envelope.offset
  }

}
