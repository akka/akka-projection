package central

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import akka.projection.grpc.producer.scaladsl.EventProducer
import central.deliveries.DeliveryEvents
import central.deliveries.RestaurantDeliveries
import central.deliveries.RestaurantDeliveriesServiceImpl
import central.drones.{ Drone, DroneOverviewServiceImpl, LocalDroneEvents }
import charging.ChargingStation
import charging.ChargingStationServiceImpl
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object Main {

  val logger = LoggerFactory.getLogger("deliveries.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem(SpawnProtocol(), "restaurant-drone-deliveries")
    try {
      init(system)
    } catch {
      case NonFatal(e) =>
        logger.error("Terminating due to initialization failure.", e)
        system.terminate()
    }
  }

  def init(implicit system: ActorSystem[SpawnProtocol.Command]): Unit = {
    val settings = DeliveriesSettings(system)
    AkkaManagement(system).start()
    ClusterBootstrap(system).start()

    Drone.init(system)
    LocalDroneEvents.initPushedEventsConsumer(system)
    RestaurantDeliveries.init(system)
    val chargingStationReplication = ChargingStation.init(system)

    val interface = system.settings.config
      .getString("restaurant-drone-deliveries-service.grpc.interface")
    val port = system.settings.config
      .getInt("restaurant-drone-deliveries-service.grpc.port")

    val pushedEventsDestination =
      LocalDroneEvents.pushedEventsDestination(system)
    val deliveryEventsProducerSource =
      DeliveryEvents.eventProducerSource(system)
    val droneOverviewService = new DroneOverviewServiceImpl(system, settings)
    val restaurantDeliveriesService =
      new RestaurantDeliveriesServiceImpl(system, settings)

    // #replicationEndpoint
    val chargingStationService = new ChargingStationServiceImpl(
      chargingStationReplication.entityRefFactory)

    // delivery events and charging station replication both are Akka Projection gRPC event
    // producers (pulled by the local drone control) and needs to be combined into a single gRPC service handling both:
    val eventPullHandler = EventProducer.grpcServiceHandler(
      Set(
        deliveryEventsProducerSource,
        chargingStationReplication.eventProducerSource))

    // the drone events from edge and the charging station replicated entity are both Akka Projection gRPC
    // event push destinations (pushed by local drone control) and needs to be combined into a single gRPC service handling both:
    val eventPushHandler = EventProducerPushDestination.grpcServiceHandler(
      Set(
        pushedEventsDestination,
        chargingStationReplication.eventProducerPushDestination.get))(system)
    // #replicationEndpoint

    DroneDeliveriesServer.start(
      interface,
      port,
      droneOverviewService,
      restaurantDeliveriesService,
      chargingStationService,
      eventPullHandler,
      eventPushHandler)

  }

}
