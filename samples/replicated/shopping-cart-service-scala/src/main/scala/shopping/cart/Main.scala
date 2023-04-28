package shopping.cart

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import org.slf4j.LoggerFactory
import scala.util.control.NonFatal

object Main {

  val logger = LoggerFactory.getLogger("shopping.cart.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem[Nothing](Behaviors.empty, "shopping-cart-service")
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

    // #single-service-handler
    val replicatedShoppingCart = ShoppingCart.init(system)
    // alternatively
    // val replicatedShoppingCart = ShoppingCart.initWithProducerFilter(system)
    val replicationService = replicatedShoppingCart.createSingleServiceHandler()
    // #single-service-handler

    val grpcInterface = system.settings.config.getString("shopping-cart-service.grpc.interface")
    val grpcPort = system.settings.config.getInt("shopping-cart-service.grpc.port")
    val grpcService = new ShoppingCartServiceImpl(system, replicatedShoppingCart.entityTypeKey)
    ShoppingCartServer.start(grpcInterface, grpcPort, system, grpcService, replicationService)
  }

}
