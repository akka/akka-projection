/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.grpc.scaladsl.Metadata
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.FilteredPayload
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.consumer.ConsumerFilter.FilterCriteria
import akka.projection.grpc.consumer.EventProducerPushDestinationSettings
import akka.projection.grpc.internal.EventPusherConsumerServiceImpl
import akka.projection.grpc.internal.ProtoAnySerialization.Prefer
import akka.projection.grpc.internal.proto.EventConsumerServicePowerApiHandler
import com.google.protobuf.Descriptors

import scala.collection.immutable
import scala.reflect.ClassTag
import akka.persistence.Persistence
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.replication.scaladsl.ReplicationSettings

/**
 * A passive consumer service for event producer push that can be bound as a gRPC endpoint accepting active producers
 * pushing events, for example to run a projection piercing firewalls or NAT. Events are pushed directly into the
 * configured journal and can then be consumed through a local projection. A producer can push events for multiple
 * entities but no two producer are allowed to push events for the same entity, at the same time.
 *
 * The event consumer service is not needed for normal projections over gRPC where the consuming side can access and
 * initiate connections to the producing side.
 *
 * Producers are started using the [[akka.projection.grpc.producer.scaladsl.EventProducerPush]] API.
 */
@ApiMayChange
object EventProducerPushDestination {

  /**
   * @param acceptedStreamId Only accept this stream ids, deny others
   * @param protobufDescriptors When using protobuf as event wire format, rather than direct Akka Serialization,
   *                            all message descriptors needs to be listed up front when creating the destination.
   *                            If not using protobuf encoded events, use an empty list.
   */
  def apply(acceptedStreamId: String, protobufDescriptors: immutable.Seq[Descriptors.FileDescriptor])(
      implicit system: ActorSystem[_]): EventProducerPushDestination =
    new EventProducerPushDestination(
      None,
      acceptedStreamId,
      (_, _) => Transformation.identity,
      None,
      immutable.Seq.empty,
      protobufDescriptors,
      EventProducerPushDestinationSettings(system),
      None)

  @ApiMayChange
  object Transformation {

    /**
     * Starting point for building `Transformation`. Registrations of actual transformations must
     * be added. Use [[Transformation.identity]] to pass through each event as is.
     */
    val empty: Transformation =
      new Transformation(Map.empty, Predef.identity)

    /**
     * No transformation. Pass through each event as is.
     */
    val identity: Transformation =
      new Transformation(Map.empty, Predef.identity)
  }

  /**
   * Transformation of incoming pushed events from the producer to the internal representation stored in the journal
   * and seen by local projections. Start from [[Transformation.empty]] when defining transformations.
   */
  final class Transformation private (
      private[akka] val typedMappers: Map[Class[_], EventEnvelope[Any] => EventEnvelope[Any]],
      untypedMappers: EventEnvelope[Any] => EventEnvelope[Any]) {

    /**
     * Add or replace tags for incoming events
     */
    def registerTagMapper[A: ClassTag](f: EventEnvelope[A] => Set[String]): Transformation = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      val mapTags = { (eventEnvelope: EventEnvelope[Any]) =>
        val newTags = f(eventEnvelope.asInstanceOf[EventEnvelope[A]])
        if (newTags eq eventEnvelope.tags) eventEnvelope
        else eventEnvelope.withTags(newTags)
      }
      appendMapper(clazz, mapTags)
    }

    /**
     * Transform incoming persistence ids, care must be taken to produce a valid persistence id and to always map the
     * same incoming persistence id to the same stored persistence id to not introduce gaps in the sequence numbers
     * and break consuming projections.
     */
    def registerPersistenceIdMapper(f: EventEnvelope[Any] => String)(
        implicit system: ActorSystem[_]): Transformation = {
      val mapId = { (eventEnvelope: EventEnvelope[Any]) =>
        val newPid = f(eventEnvelope)
        if (newPid eq eventEnvelope.persistenceId)
          eventEnvelope
        else {
          val entityType = PersistenceId.extractEntityType(newPid)
          val slice = Persistence(system).sliceForPersistenceId(newPid)
          eventEnvelope.withPersistenceId(newPid, entityType, slice)
        }
      }
      // needs to be untyped since not mapping filtered events the same way will cause gaps in seqnrs
      new Transformation(typedMappers, untypedMappers.andThen(mapId))
    }

    /**
     * Transform incoming event payloads.
     *
     * Events can be excluded by mapping the payload to `None`.
     */
    def registerMapper[A: ClassTag, B](f: A => Option[B]): Transformation =
      registerEnvelopeMapper[A, B](envelope => f(envelope.event))

    /**
     * Transform incoming event payloads, with access to the entire envelope for additional metadata.
     *
     * Events can be excluded by mapping the payload to `None`.
     */
    def registerEnvelopeMapper[A: ClassTag, B](f: EventEnvelope[A] => Option[B]): Transformation = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      val mapPayload = { (eventEnvelope: EventEnvelope[Any]) =>
        if (eventEnvelope.filtered) eventEnvelope
        else {
          val newMaybePayload = f(eventEnvelope.asInstanceOf[EventEnvelope[A]])
          if (newMaybePayload eq eventEnvelope.eventOption) eventEnvelope
          else eventEnvelope.withEventOption(newMaybePayload)
        }
      }
      appendMapper(clazz, mapPayload)
    }

    /**
     * Events can be excluded by mapping the payload to `None`.
     */
    def registerOrElsePayloadMapper(f: EventEnvelope[Any] => Option[Any]): Transformation = {
      val anyPayloadMapper = { (eventEnvelope: EventEnvelope[Any]) =>
        if (eventEnvelope.filtered) eventEnvelope
        else {
          val newMaybePayload = f(eventEnvelope)
          if (newMaybePayload eq eventEnvelope.eventOption) eventEnvelope
          else eventEnvelope.withEventOption(newMaybePayload)
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

  }

  def grpcServiceHandler(eventConsumer: EventProducerPushDestination)(
      implicit system: ActorSystem[_]): PartialFunction[HttpRequest, scala.concurrent.Future[HttpResponse]] =
    grpcServiceHandler(Set(eventConsumer))

  def grpcServiceHandler(eventConsumer: Set[EventProducerPushDestination])(
      implicit system: ActorSystem[_]): PartialFunction[HttpRequest, scala.concurrent.Future[HttpResponse]] =
    EventConsumerServicePowerApiHandler.partial(EventPusherConsumerServiceImpl(eventConsumer, Prefer.Scala))

}

@ApiMayChange
final class EventProducerPushDestination private[akka] (
    val journalPluginId: Option[String],
    val acceptedStreamId: String,
    val transformationForOrigin: (String, Metadata) => EventProducerPushDestination.Transformation,
    val interceptor: Option[EventDestinationInterceptor],
    val filters: immutable.Seq[FilterCriteria],
    val protobufDescriptors: immutable.Seq[Descriptors.FileDescriptor],
    val settings: EventProducerPushDestinationSettings,
    /** INTERNAL API */
    @InternalApi
    private[akka] val replicationSettings: Option[ReplicationSettings[_]]) {
  import EventProducerPushDestination._

  def withInterceptor(interceptor: EventDestinationInterceptor): EventProducerPushDestination =
    copy(interceptor = Some(interceptor))

  def withJournalPluginId(journalPluginId: String): EventProducerPushDestination =
    copy(journalPluginId = Some(journalPluginId))

  def withSettings(settings: EventProducerPushDestinationSettings): EventProducerPushDestination =
    copy(settings = settings)

  /**
   * @param transformation A transformation to use for all events.
   */
  def withTransformation(transformation: Transformation): EventProducerPushDestination = {
    require(
      transformation ne Transformation.empty,
      s"Transformation must not be empty. Use Transformation.identity to pass through each event as is.")
    copy(transformationForOrigin = (_, _) => transformation)
  }

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
   * Filter incoming streams, at producer side, with these filters
   */
  def withConsumerFilters(filters: immutable.Seq[FilterCriteria]): EventProducerPushDestination =
    copy(filters = filters)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def withEdgeReplication(replicationSettings: ReplicationSettings[_]): EventProducerPushDestination =
    copy(replicationSettings = Some(replicationSettings))

  private def copy(
      journalPluginId: Option[String] = journalPluginId,
      acceptedStreamId: String = acceptedStreamId,
      transformationForOrigin: (String, Metadata) => Transformation = transformationForOrigin,
      interceptor: Option[EventDestinationInterceptor] = interceptor,
      filters: immutable.Seq[FilterCriteria] = filters,
      protobufDescriptors: immutable.Seq[Descriptors.FileDescriptor] = protobufDescriptors,
      settings: EventProducerPushDestinationSettings = settings,
      replicationSettings: Option[ReplicationSettings[_]] = replicationSettings): EventProducerPushDestination =
    new EventProducerPushDestination(
      journalPluginId,
      acceptedStreamId,
      transformationForOrigin,
      interceptor,
      filters,
      protobufDescriptors,
      settings,
      replicationSettings)
}
