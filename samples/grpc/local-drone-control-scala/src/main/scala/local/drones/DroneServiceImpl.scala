package local.drones

import akka.Done
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.grpc.GrpcServiceException
import akka.util.Timeout
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.slf4j.LoggerFactory
import local.drones.proto

import scala.concurrent.Future
import scala.concurrent.TimeoutException

class DroneServiceImpl(
    deliveriesQueue: ActorRef[DeliveriesQueue.Command],
    settings: Settings)(implicit system: ActorSystem[_])
    extends proto.DroneService {

  import system.executionContext

  private val logger = LoggerFactory.getLogger(getClass)

  implicit private val timeout: Timeout = settings.askTimeout

  private val sharding = ClusterSharding(system)

  override def reportLocation(
      in: proto.ReportLocationRequest): Future[Empty] = {
    val coordinates = in.coordinates.getOrElse {
      throw new GrpcServiceException(
        Status.INVALID_ARGUMENT.withDescription(
          "coordinates are required but missing"))
    }
    logger.info(
      "Report location ({},{},{}) for drone {}",
      coordinates.latitude,
      coordinates.longitude,
      in.altitude,
      in.droneId)
    val entityRef = sharding.entityRefFor(Drone.EntityKey, in.droneId)
    val reply: Future[Done] = entityRef.ask(
      Drone.ReportPosition(
        Position(Coordinates.fromProto(coordinates), in.altitude),
        _))
    val response = reply.map(_ => Empty.defaultInstance)
    convertError(response)
  }

  // #requestNextDelivery
  override def requestNextDelivery(in: proto.RequestNextDeliveryRequest)
      : Future[proto.RequestNextDeliveryResponse] = {
    logger.info("Drone {} requesting next delivery", in.droneId)

    // get location for drone
    val entityRef = sharding.entityRefFor(Drone.EntityKey, in.droneId)

    // ask for closest delivery
    val response = for {
      position <- entityRef.askWithStatus[Position](Drone.GetCurrentPosition(_))
      chosenDelivery <- deliveriesQueue
        .askWithStatus[DeliveriesQueue.WaitingDelivery](
          DeliveriesQueue.RequestDelivery(in.droneId, position.coordinates, _))
    } yield {
      proto.RequestNextDeliveryResponse(
        deliveryId = chosenDelivery.deliveryId,
        from = Some(chosenDelivery.from.toProto),
        to = Some(chosenDelivery.to.toProto))
    }

    convertError(response)
  }
  // #requestNextDelivery
  override def completeDelivery(
      in: proto.CompleteDeliveryRequest): Future[Empty] = {
    logger.info("Delivery {} completed", in.deliveryId)

    deliveriesQueue
      .askWithStatus[Done](DeliveriesQueue.CompleteDelivery(in.deliveryId, _))
      .map(_ => Empty.defaultInstance)
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
            Status.INTERNAL.withDescription(exc.getMessage)))
    }
  }
}
