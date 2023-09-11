package iot.registration

import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation

object RegistrationEvents {

  def eventProducerService(system: ActorSystem[_])
      : PartialFunction[HttpRequest, Future[HttpResponse]] = {
    val transformation = Transformation.empty
      .registerAsyncEnvelopeMapper[Registration.Registered, proto.Registered](
        envelope => Future.successful(Some(transformRegistered(envelope))))

    val eventProducerSource = EventProducer.EventProducerSource(
      Registration.EntityKey.name,
      "registration-events",
      transformation,
      EventProducerSettings(system))

    EventProducer.grpcServiceHandler(eventProducerSource)(system)
  }

  private def transformRegistered(
      envelope: EventEnvelope[Registration.Registered]): proto.Registered = {
    val event = envelope.event
    proto.Registered(secret = Some(proto.SecretDataValue(event.secret.value)))
  }

}
