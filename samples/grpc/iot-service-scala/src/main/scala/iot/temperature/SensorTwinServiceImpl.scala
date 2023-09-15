package iot.temperature

import java.util.concurrent.TimeoutException

import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.grpc.GrpcServiceException
import akka.util.Timeout
import io.grpc.Status
import iot.temperature.proto.SensorTwinService

class SensorTwinServiceImpl(system: ActorSystem[_]) extends SensorTwinService {

  import system.executionContext

  implicit private val timeout: Timeout =
    Timeout.create(
      system.settings.config.getDuration("iot-service.ask-timeout"))

  private val sharding = ClusterSharding(system)

  override def getTemperature(
      in: proto.GetTemperatureRequest): Future[proto.CurrentTemperature] = {
    val entityRef = sharding.entityRefFor(SensorTwin.EntityKey, in.sensorEntityId)
    val reply: Future[Int] =
      entityRef.askWithStatus(SensorTwin.GetTemperature(_))
    val response =
      reply.map(temperature =>
        proto.CurrentTemperature(in.sensorEntityId, temperature))
    convertError(response)
  }

  private def convertError[T](response: Future[T]): Future[T] = {
    response.recoverWith {
      case _: TimeoutException =>
        Future.failed(
          new GrpcServiceException(
            Status.UNAVAILABLE.withDescription("Operation timed out")))
      case exc =>
        Future.failed(
          new GrpcServiceException(
            Status.INVALID_ARGUMENT.withDescription(exc.getMessage)))
    }
  }
}
