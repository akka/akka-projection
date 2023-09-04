package iot.registration

import java.util.concurrent.TimeoutException

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.grpc.GrpcServiceException
import akka.util.Timeout
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.slf4j.LoggerFactory

class RegistrationServiceImpl(system: ActorSystem[_])
    extends proto.RegistrationService {

  import system.executionContext

  private val logger = LoggerFactory.getLogger(getClass)

  implicit private val timeout: Timeout =
    Timeout.create(
      system.settings.config.getDuration("iot-service.ask-timeout"))

  private val sharding = ClusterSharding(system)

  override def register(in: proto.RegisterRequest): Future[Empty] = {
    logger.info("register sensor {}", in.sensorEntityId)
    val entityRef = sharding.entityRefFor(Registration.EntityKey, in.sensorEntityId)
    val reply: Future[Done] =
      entityRef.askWithStatus(
        Registration.Register(Registration.SecretDataValue(in.secret), _))
    val response = reply.map(_ => Empty.defaultInstance)
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
