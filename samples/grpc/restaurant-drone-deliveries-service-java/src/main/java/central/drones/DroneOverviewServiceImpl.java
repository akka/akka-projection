package central.drones;

import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.grpc.GrpcServiceException;
import akka.japi.Pair;
import akka.persistence.r2dbc.session.javadsl.R2dbcSession;
import akka.persistence.typed.PersistenceId;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import central.CoarseGrainedCoordinates;
import central.DeliveriesSettings;
import central.drones.proto.*;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public final class DroneOverviewServiceImpl implements DroneOverviewService {

  private Logger logger = LoggerFactory.getLogger(DroneOverviewServiceImpl.class);

  private final ActorSystem<?> system;
  private final DeliveriesSettings settings;
  private final Serialization serialization;
  private final ClusterSharding sharding;

  private static final String FIND_BY_LOCATION_SQL =
      "SELECT persistence_id, state_ser_id, state_ser_manifest, state_payload "
          + "FROM durable_state "
          + "WHERE location = $1";

  public DroneOverviewServiceImpl(ActorSystem<?> system, DeliveriesSettings settings) {
    this.system = system;
    this.settings = settings;
    this.serialization = SerializationExtension.get(system);
    this.sharding = ClusterSharding.get(system);
  }

  @Override
  public CompletionStage<GetDroneOverviewResponse> getDroneOverview(GetDroneOverviewRequest in) {
    logger.info("Get drone overview for drone {}", in.getDroneId());
    var entityRef = sharding.entityRefFor(Drone.ENTITY_KEY, in.getDroneId());
    CompletionStage<Drone.State> response = entityRef.ask(Drone.GetState::new, settings.droneAskTimeout);

    return response.thenApply(state -> {
        if (state.currentLocation.isPresent())
            return GetDroneOverviewResponse.newBuilder()
                    .setLocationName(state.locationName)
                    .setCoarseLatitude(state.currentLocation.get().latitude)
                    .setCoarseLongitude(state.currentLocation.get().longitude)
                    .build();
        else
            throw new GrpcServiceException(Status.NOT_FOUND.withDescription("No location known for " + in.getDroneId()));
    });
  }

  @Override
  public CompletionStage<CoarseDroneLocationsResponse> getCoarseDroneLocations(
      CoarseDroneLocationsRequest in) {
    // query against additional columns for drone
    logger.info("List drones for location {}", in.getLocation());
    CompletionStage<List<Pair<CoarseGrainedCoordinates, String>>> queryResult =
        R2dbcSession.withSession(
            system,
            session ->
                session.select(
                    session.createStatement(FIND_BY_LOCATION_SQL).bind(0, in.getLocation()),
                    row -> {
                      var serializerId = row.get("state_ser_id", Integer.class);
                      var serializerManifest = row.get("state_ser_manifest", String.class);
                      var payload = row.get("state_payload", byte[].class);
                      var state =
                          (Drone.State)
                              serialization
                                  .deserialize(payload, serializerId, serializerManifest)
                                  .get();
                      var droneId =
                          PersistenceId.extractEntityId(row.get("persistence_id", String.class));

                      // we expect it to always be present
                      var coordinates = state.currentLocation.get();
                      return Pair.create(coordinates, droneId);
                    }));

    return queryResult.thenApply(
        (List<Pair<CoarseGrainedCoordinates, String>> droneIdAndLocations) -> {
          if (droneIdAndLocations.isEmpty()) throw new GrpcServiceException(Status.NOT_FOUND);
          else {
            // group drones by coarse location
            Map<CoarseGrainedCoordinates, Set<String>> byLocation =
                droneIdAndLocations.stream()
                    .collect(
                        Collectors.toMap(
                            Pair::first,
                            pair -> new HashSet<>(Collections.singletonList(pair.second())),
                            (existingSet, newSet) -> {
                              existingSet.addAll(newSet);
                              return existingSet;
                            }));

            // turn into response protobuf message
            var protoEntries =
                byLocation.entrySet().stream()
                    .map(
                        entry ->
                            CoarseDroneLocations.newBuilder()
                                .setCoordinates(entry.getKey().toProto())
                                .addAllDrones(entry.getValue())
                                .build())
                    .collect(Collectors.toList());
            return CoarseDroneLocationsResponse.newBuilder()
                .addAllCoarseLocations(protoEntries)
                .build();
          }
        });
  }
}
