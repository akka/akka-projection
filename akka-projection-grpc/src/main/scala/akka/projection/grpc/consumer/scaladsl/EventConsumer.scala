/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.internal.EventConsumerServiceImpl
import akka.projection.grpc.internal.proto.EventConsumerServicePowerApiHandler

import scala.concurrent.Future

/**
 * The EventConsumer is an optional passive consumer service that can be bound as a gRPC endpoint accepting active producers
 * pushing events, for example to run a projection piercing firewalls or NAT. Events are pushed directly into the configured
 * journal and can then be consumed through a local projection. A producer can push events for multiple entities but no two
 * producer are allowed to push events for the same entity.
 *
 * The event consumer service is not needed for normal projections over gRPC where the consuming side can access and initiate
 * connections to the producing side.
 *
 * Producers are started using the [[akka.projection.grpc.producer.scaladsl.ActiveEventProducer]] API.
 */
// FIXME Java API
@ApiMayChange
object EventConsumer {

  final class EventConsumerDestination private (
      val journalPluginId: Option[String],
      val acceptedStreamIds: Set[String],
      val transformation: Transformation,
      val interceptor: Option[EventConsumerInterceptor] = None) {

    def withInterceptor(interceptor: EventConsumerInterceptor): EventConsumerDestination =
      copy(interceptor = Some(interceptor))

    def withJournalPluginId(journalPluginId: String): EventConsumerDestination =
      copy(journalPluginId = Some(journalPluginId))

    def withTransformation(transformation: Transformation): EventConsumerDestination =
      copy(transformation = transformation)

    private def copy(
        journalPluginId: Option[String] = journalPluginId,
        acceptedStreamIds: Set[String] = acceptedStreamIds,
        transformation: Transformation = transformation,
        interceptor: Option[EventConsumerInterceptor] = interceptor): EventConsumerDestination =
      new EventConsumerDestination(journalPluginId, acceptedStreamIds, transformation, interceptor)
  }

  object EventConsumerDestination {

    /**
     * @param acceptedStreamIds Only accept these stream ids, deny others
     */
    def apply(acceptedStreamIds: Set[String]): EventConsumerDestination =
      new EventConsumerDestination(None, acceptedStreamIds, Transformation)

    /**
     * @param acceptedStreamId Only accept this stream ids, deny others
     */
    def apply(acceptedStreamId: String): EventConsumerDestination =
      apply(Set(acceptedStreamId))
  }

  @ApiMayChange
  object Transformation extends Transformation {
    // Note: this is also the empty/identity transformation
    override private[akka] def apply(eventEnvelope: EventEnvelope[Any]): EventEnvelope[Any] =
      eventEnvelope
  }

  private final case class MapTags(f: EventEnvelope[Any] => Set[String]) extends Transformation {
    override private[akka] def apply(eventEnvelope: EventEnvelope[Any]): EventEnvelope[Any] = {
      val newTags = f(eventEnvelope)
      if (newTags eq eventEnvelope.tags) eventEnvelope
      else
        new EventEnvelope[Any](
          offset = eventEnvelope.offset,
          persistenceId = eventEnvelope.persistenceId,
          sequenceNr = eventEnvelope.sequenceNr,
          eventOption = eventEnvelope.eventOption,
          timestamp = eventEnvelope.timestamp,
          eventMetadata = eventEnvelope.eventMetadata,
          entityType = eventEnvelope.entityType,
          slice = eventEnvelope.slice,
          filtered = eventEnvelope.filtered,
          source = eventEnvelope.source,
          tags = newTags)
    }

  }

  private final case class MapPersistenceId(f: EventEnvelope[Any] => String) extends Transformation {
    override private[akka] def apply(eventEnvelope: EventEnvelope[Any]): EventEnvelope[Any] = {
      val newPid = f(eventEnvelope)
      if (newPid eq eventEnvelope.persistenceId) eventEnvelope
      else
        new EventEnvelope[Any](
          offset = eventEnvelope.offset,
          persistenceId = newPid,
          sequenceNr = eventEnvelope.sequenceNr,
          eventOption = eventEnvelope.eventOption,
          timestamp = eventEnvelope.timestamp,
          eventMetadata = eventEnvelope.eventMetadata,
          entityType = eventEnvelope.entityType,
          slice = eventEnvelope.slice,
          filtered = eventEnvelope.filtered,
          source = eventEnvelope.source,
          tags = eventEnvelope.tags)
    }
  }

  private final case class MapPayload(f: EventEnvelope[Any] => Option[Any]) extends Transformation {
    override private[akka] def apply(eventEnvelope: EventEnvelope[Any]): EventEnvelope[Any] = {
      val newMaybePayload = f(eventEnvelope)
      if (newMaybePayload eq eventEnvelope.eventOption) eventEnvelope
      else {
        new EventEnvelope[Any](
          offset = eventEnvelope.offset,
          persistenceId = eventEnvelope.persistenceId,
          sequenceNr = eventEnvelope.sequenceNr,
          eventOption = newMaybePayload,
          timestamp = eventEnvelope.timestamp,
          eventMetadata = eventEnvelope.eventMetadata,
          entityType = eventEnvelope.entityType,
          slice = eventEnvelope.slice,
          filtered = newMaybePayload.isEmpty,
          source = eventEnvelope.source,
          tags = eventEnvelope.tags)
      }

    }
  }

  private final case class AndThen(first: Transformation, next: Transformation) extends Transformation {
    override private[akka] def apply(eventEnvelope: EventEnvelope[Any]): EventEnvelope[Any] =
      next.apply(first.apply(eventEnvelope))
  }

  /**
   * Transformation of events from the producer type to the internal representation stored in the journal
   * and seen by local projections.
   *
   * Events can be excluded by mapping them to `None`.
   */
  sealed trait Transformation {
    def mapTags[Event](f: EventEnvelope[Event] => Set[String]): Transformation =
      MapTags(f.asInstanceOf[EventEnvelope[Any] => Set[String]])
    def mapPersistenceId[Event](f: EventEnvelope[Event] => String): Transformation =
      MapPersistenceId(f.asInstanceOf[EventEnvelope[Any] => String])
    def mapPayload[Event, B <: Event](f: EventEnvelope[Event] => Option[B]): Transformation =
      MapPayload(f.asInstanceOf[EventEnvelope[Any] => Option[B]])

    def andThen(transformation: Transformation): Transformation =
      AndThen(this, transformation)

    // FIXME do we need async?
    private[akka] def apply(eventEnvelope: EventEnvelope[Any]): EventEnvelope[Any]
  }

  def grpcServiceHandler(eventConsumer: EventConsumerDestination)(
      implicit system: ActorSystem[_]): HttpRequest => Future[HttpResponse] =
    EventConsumerServicePowerApiHandler(
      EventConsumerServiceImpl(
        journalPluginId = eventConsumer.journalPluginId,
        eventTransformer = eventConsumer.transformation,
        acceptedStreamIds = eventConsumer.acceptedStreamIds,
        interceptor = eventConsumer.interceptor))

  // FIXME do we need a handler taking a set of EventConsumerDestinations like for EventProducerSource?

}
