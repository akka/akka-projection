/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.grpc.scaladsl.Metadata
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.FilteredPayload
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.consumer.ConsumerFilter.FilterCriteria
import akka.projection.grpc.consumer.EventProducerPushDestinationSettings
import akka.projection.grpc.internal.EventPusherConsumerServiceImpl
import akka.projection.grpc.internal.proto.EventConsumerServicePowerApiHandler

import scala.collection.immutable
import scala.concurrent.Future
import scala.reflect.ClassTag

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
   * @param acceptedStreamId Only accept this stream ids, deny others
   */
  def apply(acceptedStreamId: String)(implicit system: ActorSystem[_]): EventProducerPushDestination =
    new EventProducerPushDestination(
      None,
      acceptedStreamId,
      (_, _) => Transformation.empty,
      None,
      immutable.Seq.empty,
      EventProducerPushDestinationSettings(system))

  @ApiMayChange
  object Transformation {
    val empty: Transformation = new Transformation(Map.empty, Predef.identity)
  }

  /**
   * Transformation of events from the producer type to the internal representation stored in the journal
   * and seen by local projections.
   */
  final class Transformation private (
      private[akka] val typedMappers: Map[Class[_], EventEnvelope[Any] => EventEnvelope[Any]],
      untypedMappers: EventEnvelope[Any] => EventEnvelope[Any]) {
    def registerTagMapper[A: ClassTag](f: EventEnvelope[A] => Set[String]): Transformation = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      val mapTags = { (eventEnvelope: EventEnvelope[Any]) =>
        val newTags = f(eventEnvelope.asInstanceOf[EventEnvelope[A]])
        if (newTags eq eventEnvelope.tags) eventEnvelope
        else copyEnvelope(eventEnvelope)(tags = newTags)
      }
      appendMapper(clazz, mapTags)
    }

    def registerPersistenceIdMapper(f: EventEnvelope[Any] => String): Transformation = {
      val mapId = { (eventEnvelope: EventEnvelope[Any]) =>
        val newPid = f(eventEnvelope)
        if (newPid eq eventEnvelope.persistenceId) eventEnvelope
        else copyEnvelope(eventEnvelope)(persistenceId = newPid)
      }
      // needs to be untyped since not mapping filtered events the same way will cause gaps in seqnrs
      new Transformation(typedMappers, untypedMappers.andThen(mapId))
    }

    /**
     * Events can be excluded by mapping them to `None`.
     */
    def registerPayloadMapper[A: ClassTag, B](f: EventEnvelope[A] => Option[B]): Transformation = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      val mapPayload = { (eventEnvelope: EventEnvelope[Any]) =>
        if (eventEnvelope.filtered) eventEnvelope
        else {
          val newMaybePayload = f(eventEnvelope.asInstanceOf[EventEnvelope[A]])
          if (newMaybePayload eq eventEnvelope.eventOption) eventEnvelope
          else copyEnvelope(eventEnvelope)(eventOption = newMaybePayload)
        }
      }
      appendMapper(clazz, mapPayload)
    }

    def registerOrElsePayloadMapper(f: EventEnvelope[Any] => Option[Any]): Transformation = {
      val anyPayloadMapper = { (eventEnvelope: EventEnvelope[Any]) =>
        if (eventEnvelope.filtered) eventEnvelope
        else {
          val newMaybePayload = f(eventEnvelope)
          if (newMaybePayload eq eventEnvelope.eventOption) eventEnvelope
          else copyEnvelope(eventEnvelope)(eventOption = newMaybePayload)
        }
      }
      new Transformation(typedMappers, untypedMappers.andThen(anyPayloadMapper))
    }

    private[akka] def apply(envelope: EventEnvelope[Any]): EventEnvelope[Any] = {
      val payloadClass = envelope.eventOption.map(_.getClass).getOrElse(FilteredPayload.getClass)
      val typedMapResult = typedMappers.get(payloadClass) match {
        case Some(mapper) => mapper(envelope)
        case None         => envelope
      }
      untypedMappers(typedMapResult)
    }

    private def appendMapper(clazz: Class[_], transformF: EventEnvelope[Any] => EventEnvelope[Any]): Transformation = {
      new Transformation(
        // chain if there are multiple ops for same type
        typedMappers.updated(clazz, typedMappers.get(clazz).map(f => f.andThen(transformF)).getOrElse(transformF)),
        untypedMappers)
    }

    private def copyEnvelope(original: EventEnvelope[Any])(
        offset: Offset = original.offset,
        persistenceId: String = original.persistenceId,
        sequenceNr: Long = original.sequenceNr,
        eventOption: Option[Any] = original.eventOption,
        timestamp: Long = original.timestamp,
        eventMetadata: Option[Any] = original.eventMetadata,
        entityType: String = original.entityType,
        slice: Int = original.slice,
        filtered: Boolean = original.filtered,
        source: String = original.source,
        tags: Set[String] = original.tags): EventEnvelope[Any] =
      new EventEnvelope[Any](
        offset,
        persistenceId,
        sequenceNr,
        eventOption,
        timestamp,
        eventMetadata,
        entityType,
        slice,
        filtered,
        source,
        tags)
  }

  def grpcServiceHandler(eventConsumer: EventProducerPushDestination)(
      implicit system: ActorSystem[_]): HttpRequest => Future[HttpResponse] =
    EventConsumerServicePowerApiHandler(new EventPusherConsumerServiceImpl(Set(eventConsumer)))

  def grpcServiceHandler(eventConsumer: Set[EventProducerPushDestination])(
      implicit system: ActorSystem[_]): HttpRequest => Future[HttpResponse] =
    EventConsumerServicePowerApiHandler(new EventPusherConsumerServiceImpl(eventConsumer))

}

final class EventProducerPushDestination private (
    val journalPluginId: Option[String],
    val acceptedStreamId: String,
    val transformationForOrigin: (String, Metadata) => EventProducerPushDestination.Transformation,
    val interceptor: Option[EventConsumerInterceptor],
    val filters: immutable.Seq[FilterCriteria],
    val settings: EventProducerPushDestinationSettings) {
  import EventProducerPushDestination._

  def withInterceptor(interceptor: EventConsumerInterceptor): EventProducerPushDestination =
    copy(interceptor = Some(interceptor))

  def withJournalPluginId(journalPluginId: String): EventProducerPushDestination =
    copy(journalPluginId = Some(journalPluginId))

  def withSettings(settings: EventProducerPushDestinationSettings): EventProducerPushDestination =
    copy(settings = settings)

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
      acceptedStreamId: String = acceptedStreamId,
      transformationForOrigin: (String, Metadata) => Transformation = transformationForOrigin,
      interceptor: Option[EventConsumerInterceptor] = interceptor,
      filters: immutable.Seq[FilterCriteria] = filters,
      settings: EventProducerPushDestinationSettings = settings): EventProducerPushDestination =
    new EventProducerPushDestination(
      journalPluginId,
      acceptedStreamId,
      transformationForOrigin,
      interceptor,
      filters,
      settings)
}
