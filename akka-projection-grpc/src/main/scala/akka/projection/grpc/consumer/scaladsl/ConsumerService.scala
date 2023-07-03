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

// FIXME Java API
@ApiMayChange
object ConsumerService {

  /**
   * Accept pushed events, write to the default journal
   * @param acceptedStreamIds Only accept these stream ids, deny others
   * @return An Akka HTTP handler for the service, needs to be bound to a port to actually handle pushes
   */
  // FIXME more flexible returning an object with a method for the handler, do we need that?
  def apply(acceptedStreamIds: Set[String])(implicit system: ActorSystem[_]): HttpRequest => Future[HttpResponse] =
    EventConsumerServiceHandler(
      EventConsumerServiceImpl
        .directJournalConsumer(
          journalPluginId = None,
          persistenceIdTransformer = identity,
          acceptedStreamIds = acceptedStreamIds))

  def apply(journalPluginId: String, acceptedStreamIds: Set[String])(
      implicit system: ActorSystem[_]): HttpRequest => Future[HttpResponse] = {
    EventConsumerServiceHandler(
      EventConsumerServiceImpl
        .directJournalConsumer(
          journalPluginId = Some(journalPluginId),
          persistenceIdTransformer = identity,
          acceptedStreamIds = acceptedStreamIds))
  }

  def apply(journalPluginId: String, acceptedStreamIds: Set[String], persistenceIdTransformer: String => String)(
      implicit system: ActorSystem[_]): HttpRequest => Future[HttpResponse] = {
    EventConsumerServiceHandler(
      EventConsumerServiceImpl
        .directJournalConsumer(
          journalPluginId = Some(journalPluginId),
          persistenceIdTransformer = persistenceIdTransformer,
          acceptedStreamIds = acceptedStreamIds))
  }

}
