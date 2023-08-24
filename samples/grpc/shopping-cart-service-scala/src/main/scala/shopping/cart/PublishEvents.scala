package shopping.cart

//#eventProducerService
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.typed
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation

import scala.concurrent.Future

object PublishEvents {

  def eventProducerService(system: ActorSystem[_])
      : PartialFunction[HttpRequest, Future[HttpResponse]] = {
    val transformation = Transformation.identity
//      .registerAsyncEnvelopeMapper[ShoppingCart.ItemUpdated, proto.ItemQuantityAdjusted](envelope =>
//        Future.successful(Some(transformItemUpdated(envelope))))
//      .registerAsyncEnvelopeMapper[ShoppingCart.CheckedOut, proto.CheckedOut](envelope =>
//        Future.successful(Some(transformCheckedOut(envelope))))

    //#withProducerFilter
    val eventProducerSource = EventProducer
      .EventProducerSource(
        "ShoppingCart",
        "cart",
        transformation,
        EventProducerSettings(system))
      //#eventProducerService
      .withProducerFilter[ShoppingCart.Event] { envelope =>
        val tags = envelope.tags
        tags.contains(ShoppingCart.MediumQuantityTag) ||
        tags.contains(ShoppingCart.LargeQuantityTag)
      }
    //#eventProducerService
    //#withProducerFilter

    EventProducer.grpcServiceHandler(eventProducerSource)(system)
  }
  //#eventProducerService

  //#transformItemUpdated
//  def transformItemUpdated(
//      envelope: EventEnvelope[ShoppingCart.ItemUpdated]): proto.ItemQuantityAdjusted = {
//    val event = envelope.event
//    proto.ItemQuantityAdjusted(
//      cartId = PersistenceId.extractEntityId(envelope.persistenceId),
//      itemId = event.itemId,
//      quantity = event.quantity)
//  }
//  //#transformItemUpdated
//
//  def transformCheckedOut(envelope: typed.EventEnvelope[ShoppingCart.CheckedOut]): proto.CheckedOut =
//    proto.CheckedOut(PersistenceId.extractEntityId(envelope.persistenceId))

  //#eventProducerService
}
//#eventProducerService
