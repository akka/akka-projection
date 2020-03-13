/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.cassandra

import scala.concurrent.Future

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.event.Logging
import akka.persistence.cassandra.ConfigSessionProvider
import akka.persistence.cassandra.session.CassandraSessionSettings
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.query.Offset
import akka.projection.eventsourced.EventSourcedRunner
import akka.projection.scaladsl.OffsetStore

class CassandraEventSourcedRunner(systemProvider: ClassicActorSystemProvider, eventProcessorId: String, tag: String)
    extends EventSourcedRunner {
  private val session = {
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

  override def offsetStore: OffsetStore[Offset, Future[Done]] =
    new CassandraOffsetStore(session, eventProcessorId, tag)
}
