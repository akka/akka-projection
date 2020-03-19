/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import akka.actor.ClassicActorSystemProvider
import akka.persistence.query.Offset
import akka.projection.scaladsl.OffsetManagedByProjectionHandler
import akka.projection.scaladsl.OffsetStore
import akka.projection.scaladsl.Projection
import akka.projection.scaladsl.ProjectionHandler

object EventSourcedProjection {

  def atLeastOnce[Event](
      systemProvider: ClassicActorSystemProvider,
      eventProcessorId: String,
      tag: String,
      projectionHandler: ProjectionHandler[EventEnvelope[Event]],
      offsetStore: OffsetStore[Offset],
      saveOffsetAfterNumberOfEvents: Int,
      saveOffsetAfterDuration: FiniteDuration)(
      implicit ec: ExecutionContext): Projection[akka.persistence.query.EventEnvelope, EventEnvelope[Event], Offset] = {
    Projection.atLeastOnce(
      systemProvider,
      new EventSourcedProvider(systemProvider, tag),
      new EventEnvelopeExtractor[Event],
      projectionHandler,
      offsetStore,
      saveOffsetAfterNumberOfEvents,
      saveOffsetAfterDuration)
  }

  def atMostOnce[Event](
      systemProvider: ClassicActorSystemProvider,
      eventProcessorId: String,
      tag: String,
      projectionHandler: ProjectionHandler[EventEnvelope[Event]],
      offsetStore: OffsetStore[Offset])(
      implicit ec: ExecutionContext): Projection[akka.persistence.query.EventEnvelope, EventEnvelope[Event], Offset] = {
    Projection.atMostOnce(
      systemProvider,
      new EventSourcedProvider(systemProvider, tag),
      new EventEnvelopeExtractor[Event],
      projectionHandler,
      offsetStore)
  }

  def exactlyOnce[Event](
      systemProvider: ClassicActorSystemProvider,
      eventProcessorId: String,
      tag: String,
      projectionHandler: ProjectionHandler[EventEnvelope[Event]] with OffsetManagedByProjectionHandler[Offset])(
      implicit ec: ExecutionContext): Projection[akka.persistence.query.EventEnvelope, EventEnvelope[Event], Offset] = {
    Projection.exactlyOnce(
      systemProvider,
      new EventSourcedProvider(systemProvider, tag),
      new EventEnvelopeExtractor[Event],
      projectionHandler)
  }

}
