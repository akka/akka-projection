/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.eventsourced.javadsl

import java.util.Optional

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.javadsl.EventsByTagQuery
import akka.projection.eventsourced.EventEnvelope
import akka.projection.javadsl.SourceProvider
import akka.stream.javadsl.Source

object EventSourcedProvider {

  def eventsByTag[Event](
      systemProvider: ClassicActorSystemProvider,
      readJournalPluginId: String,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {

    val eventsByTagQuery =
      PersistenceQuery(systemProvider).getReadJournalFor(classOf[EventsByTagQuery], readJournalPluginId)

    new EventsByTagSourceProvider(eventsByTagQuery, tag)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private class EventsByTagSourceProvider[Event](eventsByTagQuery: EventsByTagQuery, tag: String)
      extends SourceProvider[Offset, EventEnvelope[Event]] {

    override def source(offsetOpt: Optional[Offset]): Source[EventEnvelope[Event], NotUsed] =
      eventsByTagQuery
        .eventsByTag(tag, offsetOpt.orElse(NoOffset))
        .map(env => EventEnvelope(env))

    override def extractOffset(envelope: EventEnvelope[Event]): Offset = envelope.offset
  }

}
