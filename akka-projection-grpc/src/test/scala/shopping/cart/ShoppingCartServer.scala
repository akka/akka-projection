/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package shopping.cart

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import akka.actor.typed.ActorSystem
import akka.grpc.scaladsl.ServerReflection
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import shopping.cart.ShoppingCart.ItemAdded

object ShoppingCartServer {

  def start(
      interface: String,
      port: Int,
      system: ActorSystem[_],
      shoppingCartService: proto.ShoppingCartService): Unit = {
    implicit val sys: ActorSystem[_] = system
    implicit val ec: ExecutionContext =
      system.executionContext

    val cartSource = EventProducer.EventProducerSource(
      entityType = "ShoppingCart",
      streamId = "cart",
      transformation = Transformation.empty.registerAsyncMapper((event: ItemAdded) => {
        Future.successful(Some(proto.ItemAdded(event.cartId, event.itemId, event.quantity)))
      }),
      EventProducerSettings(system))

    val eventProducerService =
      EventProducer.grpcServiceHandler(Set(cartSource))

    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(
        eventProducerService,
        proto.ShoppingCartServiceHandler.partial(shoppingCartService),
        ServerReflection.partial(List(proto.ShoppingCartService)))

    val bound =
      Http()
        .newServerAt(interface, port)
        .bind(service)
        .map(_.addToCoordinatedShutdown(3.seconds))

    bound.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info("Shopping online at gRPC server {}:{}", address.getHostString, address.getPort)
      case Failure(ex) =>
        system.log.error("Failed to bind gRPC endpoint, terminating system", ex)
        system.terminate()
    }
  }

}
