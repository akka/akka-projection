package central

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import central.deliveries.RestaurantDeliveries
import central.deliveries.RestaurantDeliveriesServiceImpl
import central.drones.Drone
import central.drones.DroneOverviewServiceImpl
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object Main {

  val logger = LoggerFactory.getLogger("deliveries.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem(SpawnProtocol(), "deliveries")
    try {
      init(system)
    } catch {
      case NonFatal(e) =>
        logger.error("Terminating due to initialization failure.", e)
        system.terminate()
    }
  }

  def init(implicit system: ActorSystem[SpawnProtocol.Command]): Unit = {
    AkkaManagement(system).start()
    ClusterBootstrap(system).start()

    Drone.init(system)
    LocalDroneEvents.initPushedEventsConsumer(system)
    RestaurantDeliveries.init(system)

    val interface = system.settings.config
      .getString("restaurant-drone-deliveries-service.grpc.interface")
    val port = system.settings.config
      .getInt("restaurant-drone-deliveries-service.grpc.port")

    val pushedDroneEventsHandler =
      LocalDroneEvents.pushedEventsGrpcHandler(system)
    val droneOverviewService = new DroneOverviewServiceImpl(system)
    val restaurantDeliveriesService = new RestaurantDeliveriesServiceImpl(
      system)

    DroneDeliveriesServer.start(
      interface,
      port,
      droneOverviewService,
      restaurantDeliveriesService,
      pushedDroneEventsHandler)

  }

}
