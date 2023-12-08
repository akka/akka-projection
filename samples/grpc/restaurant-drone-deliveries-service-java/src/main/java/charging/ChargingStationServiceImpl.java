package charging;

import akka.Done;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.grpc.GrpcServiceException;
import central.DeliveriesSettings;
import charging.proto.*;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChargingStationServiceImpl implements ChargingStationService {

  private static final Logger logger = LoggerFactory.getLogger(ChargingStationServiceImpl.class);

  private final Function<String, EntityRef<ChargingStation.Command>> entityRefFactory;
  private final Duration askTimeout;

  public ChargingStationServiceImpl(
      DeliveriesSettings settings,
      Function<String, EntityRef<ChargingStation.Command>> entityRefFactory) {
    this.entityRefFactory = entityRefFactory;
    // FIXME separate setting
    this.askTimeout = settings.droneAskTimeout;
  }

  @Override
  public CompletionStage<CreateChargingStationResponse> createChargingStation(
      CreateChargingStationRequest in) {
    logger.info(
        "Creating charging station {} with {} charging slots, in location {}",
        in.getChargingStationId(),
        in.getChargingSlots(),
        in.getLocationId());

    var entityRef = entityRefFactory.apply(in.getChargingStationId());

    CompletionStage<Done> chargingStationReply =
        entityRef.askWithStatus(
            replyTo ->
                new ChargingStation.Create(in.getLocationId(), in.getChargingSlots(), replyTo),
            askTimeout);

    var response =
        chargingStationReply.thenApply(done -> CreateChargingStationResponse.getDefaultInstance());

    return convertError(response);
  }

  @Override
  public CompletionStage<GetChargingStationStateResponse> getChargingStationState(
      GetChargingStationStateRequest in) {
    logger.info("Get charging station {} state", in.getChargingStationId());
    var entityRef = entityRefFactory.apply(in.getChargingStationId());
    return entityRef
        .askWithStatus(ChargingStation.GetState::new, askTimeout)
        .thenApply(
            state ->
                GetChargingStationStateResponse.newBuilder()
                    .setLocationId(state.stationLocationId)
                    .setChargingSlots(state.chargingSlots)
                    .addAllCurrentlyChargingDrones(
                        state.dronesCharging.stream()
                            .map(
                                d ->
                                    ChargingDrone.newBuilder()
                                        .setDroneId(d.droneId)
                                        .setChargingComplete(
                                            instantToProtoTimestamp(d.chargingDone))
                                        .build())
                            .collect(Collectors.toList()))
                    .build());
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
