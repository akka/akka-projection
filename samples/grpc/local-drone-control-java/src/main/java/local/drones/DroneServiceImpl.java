package local.drones;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.grpc.GrpcServiceException;
import akka.pattern.StatusReply;
import io.grpc.Status;
import local.drones.proto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

import static akka.actor.typed.javadsl.AskPattern.*;

public class DroneServiceImpl implements DroneService {

  private static final Logger logger = LoggerFactory.getLogger(DroneServiceImpl.class);

  private final ActorRef<DeliveriesQueue.Command> deliveriesQueue;
  private final Settings settings;
  private final ActorSystem<?> system;

  private final ClusterSharding sharding;

  public DroneServiceImpl(ActorSystem<?> system, ActorRef<DeliveriesQueue.Command> deliveriesQueue, Settings settings) {
    this.system = system;
    this.deliveriesQueue = deliveriesQueue;
    this.settings = settings;
    this.sharding = ClusterSharding.get(system);
  }

  @Override
  public CompletionStage<ReportLocationResponse> reportLocation(ReportLocationRequest in) {
    var coordinates = in.getCoordinates();
    if (coordinates == null) {
      throw new GrpcServiceException(
          Status.INVALID_ARGUMENT.withDescription(
              "coordinates are required but missing"));
    }
    logger.info(
        "Report location ({},{},{}) for drone {}",
        coordinates.getLatitude(),
        coordinates.getLongitude(),
        in.getAltitude(),
        in.getDroneId());
    var entityRef = sharding.entityRefFor(Drone.ENTITY_KEY, in.getDroneId());
    CompletionStage<Done> reply = entityRef.ask(replyTo ->
        new Drone.ReportPosition(
            new Position(Coordinates.fromProto(coordinates), in.getAltitude()),
            replyTo),
        settings.askTimeout);
    var response = reply.thenApply(done -> ReportLocationResponse.getDefaultInstance());
    return convertError(response);
  }

  @Override
  public CompletionStage<RequestNextDeliveryResponse> requestNextDelivery(RequestNextDeliveryRequest in) {
    logger.info("Drone {} requesting next delivery", in.getDroneId());

    // get location for drone
    var entityRef = sharding.entityRefFor(Drone.ENTITY_KEY, in.getDroneId());
    var positionReply = entityRef.askWithStatus((ActorRef<StatusReply<Position>> replyTo) -> new Drone.GetCurrentPosition(replyTo), settings.askTimeout);

    // ask for closest delivery
    CompletionStage<DeliveriesQueue.WaitingDelivery> chosenDeliveryReply = positionReply.thenCompose(position ->
        askWithStatus(
            deliveriesQueue,
            replyTo ->
                new DeliveriesQueue.RequestDelivery(in.getDroneId(), position.coordinates, replyTo),
            settings.askTimeout,
            system.scheduler())
    );

    var response = chosenDeliveryReply.thenApply(chosenDelivery ->
            RequestNextDeliveryResponse.newBuilder()
                .setDeliveryId(chosenDelivery.deliveryId)
                .setFrom(chosenDelivery.from.toProto())
                .setTo(chosenDelivery.to.toProto())
                .build()
        );


    return convertError(response);
  }

  @Override
  public CompletionStage<CompleteDeliveryResponse> completeDelivery(CompleteDeliveryRequest in) {
    logger.info("Delivery {} completed", in.getDeliveryId());

    CompletionStage<Done> completeReply = askWithStatus(
        deliveriesQueue,
        replyTo -> new DeliveriesQueue.CompleteDelivery(in.getDeliveryId(), replyTo),
        settings.askTimeout,
        system.scheduler());


    var reply = completeReply.thenApply(done -> CompleteDeliveryResponse.getDefaultInstance());

    return convertError(reply);
  }

  private <T> CompletionStage<T> convertError(CompletionStage<T> response) {
    return response.exceptionally(error -> {
      if (error instanceof TimeoutException) {
        throw new GrpcServiceException(
            Status.UNAVAILABLE.withDescription("Operation timed out"));
      } else {
        throw new GrpcServiceException(
            Status.INTERNAL.withDescription(error.getMessage()));
      }
    });
  }
}
