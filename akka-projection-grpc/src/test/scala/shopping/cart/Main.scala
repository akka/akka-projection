package shopping.cart

import scala.concurrent.Future

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import org.slf4j.LoggerFactory
import scala.util.control.NonFatal

import akka.projection.grpc.service.EventProducerServiceImpl
import akka.projection.grpc.service.EventProducerServiceImpl.Transformation
import shopping.cart.ShoppingCart.ItemAdded

object Main {

  val logger = LoggerFactory.getLogger("shopping.cart.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem[Nothing](Behaviors.empty, "ShoppingCartService")
    try {
      init(system)
    } catch {
      case NonFatal(e) =>
        logger.error("Terminating due to initialization failure.", e)
        system.terminate()
    }
  }

  def init(system: ActorSystem[_]): Unit = {
    AkkaManagement(system).start()
    ClusterBootstrap(system).start()

    ShoppingCart.init(system)

    val grpcInterface =
      system.settings.config.getString("shopping-cart-service.grpc.interface")
    val grpcPort =
      system.settings.config.getInt("shopping-cart-service.grpc.port")

    val transformation =
      Transformation.empty.registerMapper((event: ItemAdded) => {
        Future.successful(
          Some(proto.ItemAdded(event.cartId, event.itemId, event.quantity)))
      })
    val eventProducerService =
      new EventProducerServiceImpl(system, transformation)

    val cartService = new ShoppingCartServiceImpl(system)
    ShoppingCartServer.start(
      grpcInterface,
      grpcPort,
      system,
      eventProducerService,
      cartService)
  }

}
