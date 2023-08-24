package local.drones

import akka.actor.typed.ActorSystem
import akka.grpc.scaladsl.ServerReflection
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success

object LocalDroneControlServer {

  def start(
      interface: String,
      port: Int,
      system: ActorSystem[_],
      droneService: proto.DroneService,
      deliveriesQueueService: proto.DeliveriesQueueService): Unit = {
    implicit val sys: ActorSystem[_] = system
    implicit val ec: ExecutionContext =
      system.executionContext

    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(
        proto.DroneServiceHandler.partial(droneService),
        proto.DeliveriesQueueServiceHandler.partial(deliveriesQueueService),
        // ServerReflection enabled to support grpcurl without import-path and proto parameters
        ServerReflection.partial(
          List(proto.DroneService, proto.DeliveriesQueueService)))

    val bound =
      Http()
        .newServerAt(interface, port)
        .bind(service)
        .map(_.addToCoordinatedShutdown(3.seconds))

    bound.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info(
          "Drone control gRPC server started {}:{}",
          address.getHostString,
          address.getPort)
      case Failure(ex) =>
        system.log.error("Failed to bind gRPC endpoint, terminating system", ex)
        system.terminate()
    }
  }

}
