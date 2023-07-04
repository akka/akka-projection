/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.projection.grpc.internal.EventConsumerServiceImpl
import akka.projection.grpc.internal.proto.EventConsumerServiceHandler

import scala.concurrent.Future

/**
 * The EventConsumer is a passive consumer that can be bound as a gRPC endpoint accepting active producers
 * pushing events, for example to run a projection piercing firewalls or NAT.
 */
// FIXME Java API
@ApiMayChange
object EventConsumer {

  final class EventConsumerDestination private (
      val journalPluginId: Option[String],
      val acceptedStreamIds: Set[String],
      val persistenceIdTransformer: String => String)

  object EventConsumerDestination {

    /**
     * Accept pushed events, write to the default journal
     *
     * @param acceptedStreamIds Only accept these stream ids, deny others
     * @return An Akka HTTP handler for the service, needs to be bound to a port to actually handle pushes
     */
    def apply(acceptedStreamIds: Set[String]): EventConsumerDestination =
      new EventConsumerDestination(None, acceptedStreamIds, identity[String])

    def apply(journalPluginId: String, acceptedStreamIds: Set[String]): EventConsumerDestination =
      new EventConsumerDestination(Some(journalPluginId), acceptedStreamIds, identity)

    def apply(
        journalPluginId: String,
        acceptedStreamIds: Set[String],
        persistenceIdTransformer: String => String): EventConsumerDestination =
      new EventConsumerDestination(Some(journalPluginId), acceptedStreamIds, persistenceIdTransformer)
  }

  def grpcServiceHandler(eventConsumer: EventConsumerDestination)(
      implicit system: ActorSystem[_]): HttpRequest => Future[HttpResponse] =
    EventConsumerServiceHandler(
      EventConsumerServiceImpl
        .directJournalConsumer(
          journalPluginId = eventConsumer.journalPluginId,
          persistenceIdTransformer = eventConsumer.persistenceIdTransformer,
          acceptedStreamIds = eventConsumer.acceptedStreamIds))

  // FIXME do we need a handler taking a set of EventConsumerDestinations like for EventProducerSource?

}
