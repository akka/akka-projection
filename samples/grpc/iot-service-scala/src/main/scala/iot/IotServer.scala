package iot

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

object IotServer {

  def start(
      interface: String,
      port: Int,
      system: ActorSystem[_],
      registrationService: registration.proto.RegistrationService,
      registrationEventProducerService: PartialFunction[
        HttpRequest,
        Future[HttpResponse]],
      pushedTemperatureEventsHandler: PartialFunction[
        HttpRequest,
        Future[HttpResponse]],
      sensorTwinService: temperature.proto.SensorTwinService): Unit = {
    implicit val sys: ActorSystem[_] = system
    implicit val ec: ExecutionContext =
      system.executionContext

    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(
        registrationEventProducerService,
        registration.proto.RegistrationServiceHandler
          .partial(registrationService),
        pushedTemperatureEventsHandler,
        temperature.proto.SensorTwinServiceHandler.partial(sensorTwinService),
        // ServerReflection enabled to support grpcurl without import-path and proto parameters
        ServerReflection.partial(
          List(
            registration.proto.RegistrationService,
            temperature.proto.SensorTwinService)))

    val bound =
      Http()
        .newServerAt(interface, port)
        .bind(service)
        .map(_.addToCoordinatedShutdown(3.seconds))

    bound.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info(
          "IoT Service online at gRPC server {}:{}",
          address.getHostString,
          address.getPort)
      case Failure(ex) =>
        system.log.error("Failed to bind gRPC endpoint, terminating system", ex)
        system.terminate()
    }
  }

}
