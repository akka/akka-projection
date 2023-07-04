/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
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
      val persistenceIdTransformer: String => String,
      val interceptor: Option[EventConsumerInterceptor] = None) {

    def withInterceptor(interceptor: EventConsumerInterceptor): EventConsumerDestination =
      copy(interceptor = Some(interceptor))

    def withJournalPluginId(journalPluginId: String): EventConsumerDestination =
      copy(journalPluginId = Some(journalPluginId))

    def withPersistenceIdTransformer(transformer: String => String): EventConsumerDestination =
      copy(persistenceIdTransformer = transformer)

    private def copy(
        journalPluginId: Option[String] = journalPluginId,
        acceptedStreamIds: Set[String] = acceptedStreamIds,
        persistenceIdTransformer: String => String = persistenceIdTransformer,
        interceptor: Option[EventConsumerInterceptor] = interceptor): EventConsumerDestination =
      new EventConsumerDestination(journalPluginId, acceptedStreamIds, persistenceIdTransformer, interceptor)
  }

  object EventConsumerDestination {

    /**
     * @param acceptedStreamIds Only accept these stream ids, deny others
     */
    def apply(acceptedStreamIds: Set[String]): EventConsumerDestination =
      new EventConsumerDestination(None, acceptedStreamIds, identity[String])

    /**
     * @param acceptedStreamId Only accept this stream ids, deny others
     */
    def apply(acceptedStreamId: String): EventConsumerDestination =
      apply(Set(acceptedStreamId))
  }

  def grpcServiceHandler(eventConsumer: EventConsumerDestination)(
      implicit system: ActorSystem[_]): HttpRequest => Future[HttpResponse] =
    EventConsumerServicePowerApiHandler(
      EventConsumerServiceImpl(
        journalPluginId = eventConsumer.journalPluginId,
        persistenceIdTransformer = eventConsumer.persistenceIdTransformer,
        acceptedStreamIds = eventConsumer.acceptedStreamIds,
        interceptor = eventConsumer.interceptor))

  // FIXME do we need a handler taking a set of EventConsumerDestinations like for EventProducerSource?

}
