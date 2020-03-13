/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.cassandra

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.projection.eventsourced.EventEnvelopeExtractor
import akka.projection.eventsourced.EventSourcedHandler
import akka.projection.eventsourced.EventSourcedProvider
import akka.projection.scaladsl.Projection

object CassandraEventSourcedProjection {

  def apply[Event](
      systemProvider: ClassicActorSystemProvider,
      eventProcessorId: String,
      tag: String,
      eventHandler: Event => Future[Done])(
      implicit ec: ExecutionContext): Projection[EventEnvelope, Event, Offset, Future[Done]] = {
    Projection(
      systemProvider,
      new EventSourcedProvider(systemProvider, tag),
      new EventEnvelopeExtractor[Event],
      new CassandraEventSourcedRunner(systemProvider, eventProcessorId, tag),
      new EventSourcedHandler[Event](eventHandler))
  }

}
