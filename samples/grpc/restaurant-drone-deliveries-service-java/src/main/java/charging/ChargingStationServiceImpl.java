package charging;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import central.DeliveriesSettings;
import charging.proto.*;
import com.google.protobuf.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
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

    return entityRef
        .ask(
            (ActorRef<Done> replyTo) ->
                new ChargingStation.Create(in.getLocationId(), in.getChargingSlots(), replyTo),
            askTimeout)
        .thenApply(done -> CreateChargingStationResponse.getDefaultInstance());
  }

  @Override
  public CompletionStage<GetChargingStationStateResponse> getChargingStationState(
      GetChargingStationStateRequest in) {
    logger.info("Get charging station {} state", in.getChargingStationId());
    var entityRef = entityRefFactory.apply(in.getChargingStationId());
    return entityRef
        .ask(ChargingStation.GetState::new, askTimeout)
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
}
