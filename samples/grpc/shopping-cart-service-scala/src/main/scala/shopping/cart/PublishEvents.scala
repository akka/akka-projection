package shopping.cart

//#eventProducerService
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation

import scala.concurrent.Future

object PublishEvents {

  def eventProducerService(system: ActorSystem[_])
      : PartialFunction[HttpRequest, Future[HttpResponse]] = {
    val transformation = Transformation.empty
      .registerMapper[ShoppingCart.ItemAdded, proto.ItemAdded](event =>
        Some(transformItemAdded(event)))
      .registerMapper[
        ShoppingCart.ItemQuantityAdjusted,
        proto.ItemQuantityAdjusted](event =>
        Some(transformItemQuantityAdjusted(event)))
      .registerMapper[ShoppingCart.ItemRemoved, proto.ItemRemoved](event =>
        Some(transformItemRemoved(event)))
      .registerMapper[ShoppingCart.CheckedOut, proto.CheckedOut](event =>
        Some(transformCheckedOut(event)))

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

  //#transformItemAdded
  private def transformItemAdded(
      added: ShoppingCart.ItemAdded): proto.ItemAdded =
    proto.ItemAdded(
      cartId = added.cartId,
      itemId = added.itemId,
      quantity = added.quantity)
  //#transformItemAdded

  def transformItemQuantityAdjusted(
      event: ShoppingCart.ItemQuantityAdjusted): proto.ItemQuantityAdjusted =
    proto.ItemQuantityAdjusted(
      cartId = event.cartId,
      itemId = event.itemId,
      quantity = event.newQuantity)

  def transformItemRemoved(event: ShoppingCart.ItemRemoved): proto.ItemRemoved =
    proto.ItemRemoved(cartId = event.cartId, itemId = event.itemId)

  def transformCheckedOut(event: ShoppingCart.CheckedOut): proto.CheckedOut =
    proto.CheckedOut(event.cartId)

  //#eventProducerService
}
//#eventProducerService
