/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.javadsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.grpc.javadsl.Metadata
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.projection.grpc.consumer.ConsumerFilter.FilterCriteria
import akka.projection.grpc.consumer.EventProducerPushDestinationSettings
import akka.projection.grpc.internal.EventPusherConsumerServiceImpl
import akka.projection.grpc.internal.proto.EventConsumerServicePowerApiHandler
import akka.projection.grpc.consumer.scaladsl
import akka.util.ccompat.JavaConverters._
import akka.japi.function.{ Function => JapiFunction }
import akka.projection.grpc.internal.ProtoAnySerialization.Prefer
import com.google.protobuf.Descriptors

import java.util.Collections
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.BiFunction
import java.util.{ List => JList, Set => JSet }
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.compat.java8.FutureConverters.FutureOps
import scala.compat.java8.OptionConverters.RichOptionalGeneric

/**
 * A passive consumer service for event producer push that can be bound as a gRPC endpoint accepting active producers
 * pushing events, for example to run a projection piercing firewalls or NAT. Events are pushed directly into the
 * configured journal and can then be consumed through a local projection. A producer can push events for multiple
 * entities but no two producer are allowed to push events for the same entity, at the same time.
 *
 * The event consumer service is not needed for normal projections over gRPC where the consuming side can access and
 * initiate connections to the producing side.
 *
 * Producers are started using the [[akka.projection.grpc.producer.javadsl.EventProducerPush]] API.
 */
@ApiMayChange
object EventProducerPushDestination {

  /**
   * @param acceptedStreamId The stream id that the producers must use for this destination
   * @param protobufDescriptors When using protobuf as event wire format, rather than direct Akka Serialization,
   *                            all message descriptors needs to be listed up front when creating the destination.
   *                            If not using protobuf encoded events, use an empty list.
   */
  def create(
      acceptedStreamId: String,
      protobufDescriptors: JList[Descriptors.FileDescriptor],
      system: ActorSystem[_]): EventProducerPushDestination =
    new EventProducerPushDestination(
      Optional.empty(),
      acceptedStreamId,
      (_, _) => Transformation.empty,
      Optional.empty(),
      Collections.emptyList(),
      protobufDescriptors,
      EventProducerPushDestinationSettings.create(system))

  def grpcServiceHandler(
      eventConsumer: EventProducerPushDestination,
      system: ActorSystem[_]): JapiFunction[HttpRequest, CompletionStage[HttpResponse]] =
    grpcServiceHandler(Collections.singleton(eventConsumer), system)

  def grpcServiceHandler(
      eventConsumers: JSet[EventProducerPushDestination],
      system: ActorSystem[_]): JapiFunction[HttpRequest, CompletionStage[HttpResponse]] = {

    val scalaConsumers = eventConsumers.asScala.map(_.asScala).toSet
    val handler =
      EventConsumerServicePowerApiHandler(new EventPusherConsumerServiceImpl(scalaConsumers, Prefer.Java)(system))(
        system)
    new JapiFunction[HttpRequest, CompletionStage[HttpResponse]] {
      override def apply(request: HttpRequest): CompletionStage[HttpResponse] =
        handler(request.asInstanceOf[akka.http.scaladsl.model.HttpRequest])
          .map(_.asInstanceOf[HttpResponse])(ExecutionContexts.parasitic)
          .toJava
    }
  }

}

@ApiMayChange
final class EventProducerPushDestination private (
    val journalPluginId: Optional[String],
    val acceptedStreamId: String,
    val transformationForOrigin: BiFunction[String, Metadata, Transformation],
    val interceptor: Optional[EventDestinationInterceptor],
    val filters: java.util.List[FilterCriteria],
    val protobufDescriptors: JList[Descriptors.FileDescriptor],
    val settings: EventProducerPushDestinationSettings) {
  def withInterceptor(interceptor: EventDestinationInterceptor): EventProducerPushDestination =
    copy(interceptor = Optional.of(interceptor))

  def withJournalPluginId(journalPluginId: String): EventProducerPushDestination =
    copy(journalPluginId = Optional.of(journalPluginId))

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
      transformationForOrigin: BiFunction[String, Metadata, Transformation]): EventProducerPushDestination =
    copy(transformationForOrigin = transformationForOrigin)

  /**
   * Filter incoming streams, at producer side, with these filters
   */
  def withConsumerFilters(filters: JList[FilterCriteria]): EventProducerPushDestination =
    copy(filters = filters)

  private def copy(
      journalPluginId: Optional[String] = journalPluginId,
      acceptedStreamId: String = acceptedStreamId,
      transformationForOrigin: BiFunction[String, Metadata, Transformation] = transformationForOrigin,
      interceptor: Optional[EventDestinationInterceptor] = interceptor,
      filters: JList[FilterCriteria] = filters,
      protobufDescriptors: JList[Descriptors.FileDescriptor] = protobufDescriptors,
      settings: EventProducerPushDestinationSettings = settings): EventProducerPushDestination =
    new EventProducerPushDestination(
      journalPluginId,
      acceptedStreamId,
      transformationForOrigin,
      interceptor,
      filters,
      protobufDescriptors,
      settings)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def asScala: scaladsl.EventProducerPushDestination =
    new scaladsl.EventProducerPushDestination(
      journalPluginId.asScala,
      acceptedStreamId,
      (origin, meta) => transformationForOrigin.apply(origin, meta.asInstanceOf[akka.grpc.javadsl.Metadata]).delegate,
      interceptor.asScala.map(javaInterceptor =>
        (streamId, meta) => javaInterceptor.intercept(streamId, meta.asInstanceOf[akka.grpc.javadsl.Metadata]).toScala),
      filters.asScala.toVector,
      protobufDescriptors.asScala.toVector,
      settings)
}
