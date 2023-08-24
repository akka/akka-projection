/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package central.deliveries

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation

import scala.concurrent.Future

object DeliveryEvents {

  def eventProducerService(system: ActorSystem[_])
      : PartialFunction[HttpRequest, Future[HttpResponse]] = {
    val transformation = Transformation.empty
      .registerAsyncEnvelopeMapper[
        RestaurantDeliveries.DeliveryRegistered,
        proto.DeliveryRegistered](envelope =>
        Future.successful(Some(transformDeliveryRegistration(envelope))))
      // filter all other types of events for the RestaurantDeliveries
      .registerOrElseMapper(_ => None)

    val eventProducerSource = EventProducer.EventProducerSource(
      RestaurantDeliveries.EntityKey.name,
      // Note: stream id used in consumer to consume this specific stream
      "delivery-events",
      transformation,
      EventProducerSettings(system))

    EventProducer.grpcServiceHandler(eventProducerSource)(system)
  }

  private def transformDeliveryRegistration(
      envelope: EventEnvelope[RestaurantDeliveries.DeliveryRegistered])
      : proto.DeliveryRegistered = {
    val delivery = envelope.event.delivery
    proto.DeliveryRegistered(
      deliveryId = delivery.deliveryId,
      origin = Some(delivery.origin.toProto),
      destination = Some(delivery.destination.toProto))
  }

}
