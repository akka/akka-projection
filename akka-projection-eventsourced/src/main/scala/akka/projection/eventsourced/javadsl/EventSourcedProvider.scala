/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.eventsourced.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.FutureConverters._

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

    new EventsByTagSourceProvider(systemProvider, eventsByTagQuery, tag)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private class EventsByTagSourceProvider[Event](
      systemProvider: ClassicActorSystemProvider,
      eventsByTagQuery: EventsByTagQuery,
      tag: String)
      extends SourceProvider[Offset, EventEnvelope[Event]] {
    implicit val dispatcher: ExecutionContext = systemProvider.classicSystem.dispatcher

    override def source(
        offsetAsync: () => CompletionStage[Optional[Offset]]): CompletionStage[Source[EventEnvelope[Event], _]] = {
      val source: Future[Source[EventEnvelope[Event], _]] = offsetAsync().asScala.map { offsetOpt =>
        eventsByTagQuery
          .eventsByTag(tag, offsetOpt.orElse(NoOffset))
          .map(env => EventEnvelope(env))
      }
      source.asJava
    }

    override def extractOffset(envelope: EventEnvelope[Event]): Offset = envelope.offset
  }

}
