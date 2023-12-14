package local.drones;

import static akka.actor.typed.javadsl.AskPattern.*;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.grpc.GrpcServiceException;
import akka.pattern.StatusReply;
import charging.ChargingStation;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import local.drones.proto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroneServiceImpl implements DroneService {

  private static final Logger logger = LoggerFactory.getLogger(DroneServiceImpl.class);

  private final ActorRef<DeliveriesQueue.Command> deliveriesQueue;
  private final Function<String, EntityRef<ChargingStation.Command>>
      chargingStationEntityRefFactory;
  private final Settings settings;
  private final ActorSystem<?> system;

  private final ClusterSharding sharding;

  public DroneServiceImpl(
      ActorSystem<?> system,
      ActorRef<DeliveriesQueue.Command> deliveriesQueue,
      Function<String, EntityRef<ChargingStation.Command>> chargingStationEntityRefFactory,
      Settings settings) {
    this.system = system;
    this.deliveriesQueue = deliveriesQueue;
    this.chargingStationEntityRefFactory = chargingStationEntityRefFactory;
    this.settings = settings;
    this.sharding = ClusterSharding.get(system);
  }

  @Override
  public CompletionStage<ReportLocationResponse> reportLocation(ReportLocationRequest in) {
    var coordinates = in.getCoordinates();
    if (coordinates == null) {
      throw new GrpcServiceException(
          Status.INVALID_ARGUMENT.withDescription("coordinates are required but missing"));
    }
    logger.info(
        "Report location ({},{},{}) for drone {}",
        coordinates.getLatitude(),
        coordinates.getLongitude(),
        in.getAltitude(),
        in.getDroneId());
    var entityRef = sharding.entityRefFor(Drone.ENTITY_KEY, in.getDroneId());
    CompletionStage<Done> reply =
        entityRef.ask(
            replyTo ->
                new Drone.ReportPosition(
                    new Position(Coordinates.fromProto(coordinates), in.getAltitude()), replyTo),
            settings.askTimeout);
    var response = reply.thenApply(done -> ReportLocationResponse.getDefaultInstance());
    return convertError(response);
  }

  // #requestNextDelivery
  @Override
  public CompletionStage<RequestNextDeliveryResponse> requestNextDelivery(
      RequestNextDeliveryRequest in) {
    logger.info("Drone {} requesting next delivery", in.getDroneId());

    // get location for drone
    var entityRef = sharding.entityRefFor(Drone.ENTITY_KEY, in.getDroneId());
    var positionReply =
        entityRef.askWithStatus(
            (ActorRef<StatusReply<Position>> replyTo) -> new Drone.GetCurrentPosition(replyTo),
            settings.askTimeout);

    // ask for closest delivery
    CompletionStage<DeliveriesQueue.WaitingDelivery> chosenDeliveryReply =
        positionReply.thenCompose(
            position ->
                askWithStatus(
                    deliveriesQueue,
                    replyTo ->
                        new DeliveriesQueue.RequestDelivery(
                            in.getDroneId(), position.coordinates, replyTo),
                    settings.askTimeout,
                    system.scheduler()));

    var response =
        chosenDeliveryReply.thenApply(
            chosenDelivery ->
                RequestNextDeliveryResponse.newBuilder()
                    .setDeliveryId(chosenDelivery.deliveryId)
                    .setFrom(chosenDelivery.from.toProto())
                    .setTo(chosenDelivery.to.toProto())
                    .build());

    return convertError(response);
  }

  // #requestNextDelivery
  @Override
  public CompletionStage<CompleteDeliveryResponse> completeDelivery(CompleteDeliveryRequest in) {
    logger.info("Delivery {} completed", in.getDeliveryId());

    CompletionStage<Done> completeReply =
        askWithStatus(
            deliveriesQueue,
            replyTo -> new DeliveriesQueue.CompleteDelivery(in.getDeliveryId(), replyTo),
            settings.askTimeout,
            system.scheduler());

    var reply = completeReply.thenApply(done -> CompleteDeliveryResponse.getDefaultInstance());

    return convertError(reply);
  }

  @Override
  public CompletionStage<ChargingResponse> goCharge(GoChargeRequest in) {
    logger.info("Requesting charge of {} from {}", in.getDroneId(), in.getChargingStationId());
    var entityRef = chargingStationEntityRefFactory.apply(in.getChargingStationId());

    CompletionStage<ChargingStation.StartChargingResponse> chargingStationResponse =
        entityRef.askWithStatus(
            replyTo -> new ChargingStation.StartCharging(in.getDroneId(), replyTo),
            settings.askTimeout);

    var response =
        chargingStationResponse.thenApply(
            message -> {
              if (message instanceof ChargingStation.ChargingStarted) {
                var expectedComplete = ((ChargingStation.ChargingStarted) message).expectedComplete;
                return ChargingResponse.newBuilder()
                    .setStarted(
                        ChargingStarted.newBuilder()
                            .setExpectedComplete(instantToProtoTimestamp(expectedComplete))
                            .build())
                    .build();
              } else if (message instanceof ChargingStation.AllSlotsBusy) {
                var firstSlotFreeAt = ((ChargingStation.AllSlotsBusy) message).firstSlotFreeAt;
                return ChargingResponse.newBuilder()
                    .setComeBackLater(
                        ComeBackLater.newBuilder()
                            .setFirstSlotFreeAt(instantToProtoTimestamp(firstSlotFreeAt))
                            .build())
                    .build();
              } else {
                throw new IllegalArgumentException(
                    "Unexpected response type " + message.getClass());
              }
            });

    return convertError(response);
  }

  @Override
  public CompletionStage<CompleteChargingResponse> completeCharge(CompleteChargeRequest in) {
    logger.info(
        "Requesting complete charging of {} from {}", in.getDroneId(), in.getChargingStationId());
    var entityRef = chargingStationEntityRefFactory.apply(in.getChargingStationId());

    CompletionStage<Done> chargingStationResponse =
        entityRef.askWithStatus(
            replyTo -> new ChargingStation.CompleteCharging(in.getDroneId(), replyTo),
            settings.askTimeout);

    var response =
        chargingStationResponse.thenApply(done -> CompleteChargingResponse.getDefaultInstance());

    return convertError(response);
  }

  private static Timestamp instantToProtoTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  private <T> CompletionStage<T> convertError(CompletionStage<T> response) {
    return response.exceptionally(
        error -> {
          if (error instanceof TimeoutException) {
            throw new GrpcServiceException(
                Status.UNAVAILABLE.withDescription("Operation timed out"));
          } else {
            throw new GrpcServiceException(Status.INTERNAL.withDescription(error.getMessage()));
          }
        });
  }
}
