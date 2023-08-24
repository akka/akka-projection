package central.deliveries

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.grpc.GrpcServiceException
import akka.pattern.StatusReply
import akka.util.Timeout
import central.{ Coordinates, DeliveriesSettings }
import io.grpc.Status
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.DurationConverters.JavaDurationOps

class RestaurantDeliveriesServiceImpl(
    system: ActorSystem[_],
    settings: DeliveriesSettings)
    extends proto.RestaurantDeliveriesService {

  private val logger = LoggerFactory.getLogger(getClass)
  private val sharding = ClusterSharding(system)

  private implicit val ec: ExecutionContext = system.executionContext
  private implicit val timeout: Timeout = system.settings.config
    .getDuration(
      "restaurant-drone-deliveries-service.restaurant-deliveries-ask-timeout")
    .toScala

  override def setUpRestaurant(in: proto.SetUpRestaurantRequest)
      : Future[proto.RegisterRestaurantResponse] = {
    logger.info(
      "Set up restaurant {}, coordinates {}, location [{}]",
      in.restaurantId,
      in.coordinates,
      in.localControlLocationId)

    if (!settings.locationIds.contains(in.localControlLocationId)) {
      throw new GrpcServiceException(Status.INVALID_ARGUMENT.withDescription(
        s"The local control location id ${in.localControlLocationId} is not known to the service"))
    }

    val entityRef =
      sharding.entityRefFor(RestaurantDeliveries.EntityKey, in.restaurantId)

    val coordinates = toCoordinates(in.coordinates)
    val reply =
      entityRef.ask(
        RestaurantDeliveries
          .SetUpRestaurant(in.localControlLocationId, coordinates, _))

    reply.map {
      case StatusReply.Error(error) =>
        throw new GrpcServiceException(
          Status.INTERNAL.withDescription(error.getMessage))
      case _ =>
        proto.RegisterRestaurantResponse()
    }
  }

  override def registerDelivery(in: proto.RegisterDeliveryRequest)
      : Future[proto.RegisterDeliveryResponse] = {
    logger.info(
      "Register delivery for restaurant {}, delivery id {}, destination {}",
      in.restaurantId,
      in.deliveryId,
      in.coordinates.get)

    val entityRef =
      sharding.entityRefFor(RestaurantDeliveries.EntityKey, in.restaurantId)

    val destination = toCoordinates(in.coordinates)

    val reply = entityRef.ask(
      RestaurantDeliveries.RegisterDelivery(in.deliveryId, destination, _))

    reply.map {
      case StatusReply.Error(error) =>
        throw new GrpcServiceException(
          Status.INTERNAL.withDescription(error.getMessage))
      case _ => proto.RegisterDeliveryResponse()
    }

  }

  private def toCoordinates(
      protoCoordinates: Option[common.proto.Coordinates]): Coordinates =
    protoCoordinates match {
      case Some(pc) => Coordinates.fromProto(pc)
      case None =>
        throw new GrpcServiceException(
          Status.INVALID_ARGUMENT.withDescription("Missing coordinates"))

    }
}
