/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.grpc.scaladsl.Metadata
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.consumer.ConsumerFilter.FilterCriteria
import akka.projection.grpc.internal.EventConsumerServiceImpl
import akka.projection.grpc.internal.proto.EventConsumerServicePowerApiHandler

import scala.collection.immutable
import scala.concurrent.Future

/**
 * A passive consumer service for event producer push that can be bound as a gRPC endpoint accepting active producers
 * pushing events, for example to run a projection piercing firewalls or NAT. Events are pushed directly into the
 * configured journal and can then be consumed through a local projection. A producer can push events for multiple
 * entities but no two producer are allowed to push events for the same entity.
 *
 * The event consumer service is not needed for normal projections over gRPC where the consuming side can access and
 * initiate connections to the producing side.
 *
 * Producers are started using the [[akka.projection.grpc.producer.scaladsl.EventProducerPush]] API.
 */
// FIXME Java API
@ApiMayChange
object EventProducerPushDestination {

  /**
   * @param acceptedStreamIds Only accept these stream ids, deny others
   */
  def apply(acceptedStreamIds: Set[String]): EventProducerPushDestination =
    new EventProducerPushDestination(None, acceptedStreamIds, (_, _) => Transformation, None, immutable.Seq.empty)

  /**
   * @param acceptedStreamId Only accept this stream ids, deny others
   */
  def apply(acceptedStreamId: String): EventProducerPushDestination =
    apply(Set(acceptedStreamId))
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

  def grpcServiceHandler(eventConsumer: EventProducerPushDestination)(
      implicit system: ActorSystem[_]): HttpRequest => Future[HttpResponse] =
    EventConsumerServicePowerApiHandler(new EventConsumerServiceImpl(eventConsumer))

  // FIXME do we need a handler taking a set of EventConsumerDestinations like for EventProducerSource?

}

final class EventProducerPushDestination private (
    val journalPluginId: Option[String],
    val acceptedStreamIds: Set[String],
    val transformationForOrigin: (String, Metadata) => EventProducerPushDestination.Transformation,
    val interceptor: Option[EventConsumerInterceptor],
    val filters: immutable.Seq[FilterCriteria]) {
  import EventProducerPushDestination._

  def withInterceptor(interceptor: EventConsumerInterceptor): EventProducerPushDestination =
    copy(interceptor = Some(interceptor))

  def withJournalPluginId(journalPluginId: String): EventProducerPushDestination =
    copy(journalPluginId = Some(journalPluginId))

  /**
   * @param transformation A transformation to use for all events.
   */
  def withTransformation(transformation: Transformation): EventProducerPushDestination =
    copy(transformationForOrigin = (_, _) => transformation)

  /**
   * @param transformation A function to create a transformation from the origin id and request metadata
   *                       of an active event producer connecting to the consumer. Invoked once per stream
   *                       so that transformations can be individual to each producer, for example modify
   *                       the persistence id or tags to include the origin id.
   */
  def withTransformationForOrigin(
      transformationForOrigin: (String, Metadata) => Transformation): EventProducerPushDestination =
    copy(transformationForOrigin = transformationForOrigin)

  /**
   * FIXME we may want to remove this and make the consumer filters dynamic just like for "normal consumers"
   */
  def withConsumerFilters(filters: immutable.Seq[FilterCriteria]): EventProducerPushDestination =
    copy(filters = filters)

  private def copy(
      journalPluginId: Option[String] = journalPluginId,
      acceptedStreamIds: Set[String] = acceptedStreamIds,
      transformationForOrigin: (String, Metadata) => Transformation = transformationForOrigin,
      interceptor: Option[EventConsumerInterceptor] = interceptor,
      filters: immutable.Seq[FilterCriteria] = filters): EventProducerPushDestination =
    new EventProducerPushDestination(journalPluginId, acceptedStreamIds, transformationForOrigin, interceptor, filters)
}
