/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.cassandra

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.event.Logging
import akka.persistence.cassandra.ConfigSessionProvider
import akka.persistence.cassandra.session.CassandraSessionSettings
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.query.Offset
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.EventEnvelopeExtractor
import akka.projection.eventsourced.EventSourcedProvider
import akka.projection.scaladsl.Projection
import akka.projection.scaladsl.ProjectionHandler

object CassandraEventSourcedProjection {

  def atLeastOnce[Event](
      systemProvider: ClassicActorSystemProvider,
      eventProcessorId: String,
      tag: String,
      projectionHandler: ProjectionHandler[EventEnvelope[Event]],
      afterNumberOfEvents: Int,
      orAfterDuration: FiniteDuration)(
      implicit ec: ExecutionContext): Projection[akka.persistence.query.EventEnvelope, EventEnvelope[Event], Offset] = {
    val offsetStore = new CassandraOffsetStore(session(systemProvider), eventProcessorId, tag)
    Projection.atLeastOnce(
      systemProvider,
      new EventSourcedProvider(systemProvider, tag),
      new EventEnvelopeExtractor[Event],
      projectionHandler,
      offsetStore,
      afterNumberOfEvents,
      orAfterDuration)
  }

  def atMostOnce[Event](
      systemProvider: ClassicActorSystemProvider,
      eventProcessorId: String,
      tag: String,
      projectionHandler: ProjectionHandler[EventEnvelope[Event]])(
      implicit ec: ExecutionContext): Projection[akka.persistence.query.EventEnvelope, EventEnvelope[Event], Offset] = {
    val offsetStore = new CassandraOffsetStore(session(systemProvider), eventProcessorId, tag)
    Projection.atMostOnce(
      systemProvider,
      new EventSourcedProvider(systemProvider, tag),
      new EventEnvelopeExtractor[Event],
      projectionHandler,
      offsetStore)
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
