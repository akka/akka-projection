/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.jdbc

import scala.concurrent.ExecutionContext

import akka.actor.ClassicActorSystemProvider
import akka.persistence.query.Offset
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.EventEnvelopeExtractor
import akka.projection.eventsourced.EventSourcedProvider
import akka.projection.scaladsl.Projection

object JdbcEventSourcedProjection {

  def onceAndOnlyOnce[Event](
      systemProvider: ClassicActorSystemProvider,
      eventProcessorId: String,
      tag: String,
      projectionHandler: JdbcSingleEventHandlerWithTxOffset[Event])(
      implicit ec: ExecutionContext): Projection[akka.persistence.query.EventEnvelope, EventEnvelope[Event], Offset] = {
    Projection.onceAndOnlyOnce(
      systemProvider,
      new EventSourcedProvider(systemProvider, tag),
      new EventEnvelopeExtractor[Event],
      projectionHandler)
  }

}
