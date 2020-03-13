/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced

import akka.actor.ClassicActorSystemProvider
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

class EventSourcedProvider(systemProvider: ClassicActorSystemProvider, tag: String)
    extends SourceProvider[Offset, EventEnvelope] {

  private val readJournalPluginId = "something.query" // FIXME setting

  private val query = PersistenceQuery(systemProvider).readJournalFor[EventsByTagQuery](readJournalPluginId)

  override def source(offset: Option[Offset]): Source[EventEnvelope, _] = {
    query.eventsByTag(tag, offset.getOrElse(Offset.noOffset))
  }
}
