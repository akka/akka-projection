/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.jdbc

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.event.Logging
import akka.persistence.cassandra.ConfigSessionProvider
import akka.persistence.cassandra.session.CassandraSessionSettings
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.projection.eventsourced.EventEnvelopeExtractor
import akka.projection.eventsourced.EventSourcedProvider
import akka.projection.scaladsl.OffsetStore
import akka.projection.scaladsl.Projection
import akka.projection.scaladsl.ProjectionHandler

object JdbcEventSourcedProjection {

  def apply[Event](
      systemProvider: ClassicActorSystemProvider,
      eventProcessorId: String,
      tag: String,
      projectionHandler: JdbcSingleEventHandlerWithTxOffset[Event])(
      implicit ec: ExecutionContext): Projection[EventEnvelope, Event, Offset] = {
    Projection(
      systemProvider,
      new EventSourcedProvider(systemProvider, tag),
      new EventEnvelopeExtractor[Event],
      projectionHandler,
      OffsetStore.OnceAnOnlyOnce,
      OffsetStore.noOffsetStore[Offset])
  }

  private def session(systemProvider: ClassicActorSystemProvider): CassandraSession = {
    // FIXME this will change with APC 1.0 / Alpakka Cassandra
    val system = systemProvider.classicSystem
    val sessionConfig = system.settings.config.getConfig("cassandra-journal")
    new CassandraSession(
      system,
      new ConfigSessionProvider(system, sessionConfig),
      CassandraSessionSettings(sessionConfig),
      system.dispatcher,
      Logging(system, getClass),
      metricsCategory = "sample",
      init = _ => Future.successful(Done))
  }

}
