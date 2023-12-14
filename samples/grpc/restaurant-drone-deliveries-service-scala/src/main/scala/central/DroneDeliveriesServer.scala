package central

import akka.actor.typed.ActorSystem
import akka.grpc.scaladsl.ServerReflection
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import central.Main.logger
import central.deliveries.proto.RestaurantDeliveriesService
import central.deliveries.proto.RestaurantDeliveriesServiceHandler
import central.drones.proto.DroneOverviewService
import central.drones.proto.DroneOverviewServiceHandler
import charging.proto.ChargingStationService
import charging.proto.ChargingStationServiceHandler

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

object DroneDeliveriesServer {

  def start(
      interface: String,
      port: Int,
      droneOverviewService: central.drones.proto.DroneOverviewService,
      restaurantDeliveriesService: central.deliveries.proto.RestaurantDeliveriesService,
      chargingStationService: charging.proto.ChargingStationService,
      eventPullHandler: PartialFunction[HttpRequest, Future[HttpResponse]],
      eventPushHandler: PartialFunction[HttpRequest, Future[HttpResponse]])(
      implicit system: ActorSystem[_]): Unit = {
    import system.executionContext

    // #composeAndBind
    val service = ServiceHandler.concatOrNotFound(
      DroneOverviewServiceHandler.partial(droneOverviewService),
      RestaurantDeliveriesServiceHandler.partial(restaurantDeliveriesService),
      ChargingStationServiceHandler.partial(chargingStationService),
      eventPullHandler,
      eventPushHandler,
      ServerReflection.partial(
        List(
          DroneOverviewService,
          RestaurantDeliveriesService,
          ChargingStationService)))

    val bound = Http(system).newServerAt(interface, port).bind(service)
    // #composeAndBind
    bound.foreach(binding =>
      logger.info(
        "Drone event consumer listening at: {}:{}",
        binding.localAddress.getHostString,
        binding.localAddress.getPort))

    bound.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info(
          "Drone Deliveries Service gRPC server started at {}:{}",
          address.getHostString,
          address.getPort)
      case Failure(ex) =>
        system.log.error("Failed to bind gRPC endpoint, terminating system", ex)
        system.terminate()
    }

  }

}
