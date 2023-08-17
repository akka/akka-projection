package deliveries

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

object Main {

  val logger = LoggerFactory.getLogger("deliveries.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem[Nothing](Behaviors.empty, "deliveries")
    try {
      init(system)
    } catch {
      case NonFatal(e) =>
        logger.error("Terminating due to initialization failure.", e)
        system.terminate()
    }
  }


  def init(implicit system: ActorSystem[_]): Unit = {
    implicit val ec: ExecutionContext = system.executionContext
    AkkaManagement(system).start()
    ClusterBootstrap(system).start()

    val interface = system.settings.config.getString("restaurant-drone-deliveries-service.grpc.interface")
    val port = system.settings.config.getInt("restaurant-drone-deliveries-service.grpc.port")

    // consumer runs gRPC server accepting pushed events from producers
    val streamId = "drone-events"
    val destination = EventProducerPushDestination(streamId)
    val bound = Http(system)
      .newServerAt(interface, port)
      .bind(EventProducerPushDestination.grpcServiceHandler(destination))
    bound.foreach(binding =>
      logger.info("Drone event consumer listening at: {}:{}", binding.localAddress.getHostString, binding.localAddress.getPort))
  }

}
