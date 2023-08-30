package central.drones;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;
import akka.persistence.typed.PersistenceId;
import akka.projection.Projection;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.consumer.javadsl.EventProducerPushDestination;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import central.CoarseGrainedCoordinates;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import local.drones.proto.CoarseDroneLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LocalDroneEvents {

  private static final Logger logger = LoggerFactory.getLogger(LocalDroneEvents.class);
  public static final String DRONE_EVENT_STREAM_ID = "drone-events";

  // FIXME The type key on the producer side. Make sure we have documented it.
  private static final String PRODUCER_ENTITY_TYPE = "Drone";

  public static Function<HttpRequest, CompletionStage<HttpResponse>> pushedEventsGrpcHandler(
      ActorSystem<?> system) {
    var destination =
        EventProducerPushDestination.create(
                DRONE_EVENT_STREAM_ID,
                Collections.singletonList(local.drones.proto.DroneEvents.getDescriptor()),
                system)
            .withTransformationForOrigin(
                (origin, metadataa) ->
                    akka.projection.grpc.consumer.javadsl.Transformation.empty()
                        // tag all events with the location name of the local control it came from
                        .registerTagMapper(
                            local.drones.proto.CoarseDroneLocation.class,
                            envelope -> Collections.singleton("location:" + origin)));

    return EventProducerPushDestination.grpcServiceHandler(destination, system);
  }

  private static class LocationHandler extends Handler<EventEnvelope<CoarseDroneLocation>> {

    private final ClusterSharding sharding;
    private final Duration askTimeout;

    public LocationHandler(ActorSystem<?> system) {
      this.sharding = ClusterSharding.get(system);
      this.askTimeout =
          system
              .settings()
              .config()
              .getDuration("restaurant-drone-deliveries-service.drone-ask-timeout");
    }

    @Override
    public CompletionStage<Done> process(EventEnvelope<CoarseDroneLocation> envelope) {
      logger.info(
          "Saw projected event: {}-{}: {}",
          envelope.persistenceId(),
          envelope.sequenceNr(),
          envelope.eventOption());

      // Drone id without producer entity key
      var droneId = PersistenceId.extractEntityId(envelope.persistenceId());

      // same drone id as local but different entity type (our Drone overview representation)
      var entityRef = sharding.entityRefFor(Drone.ENTITY_KEY, droneId);

      // we have encoded origin in a tag, extract it
      // FIXME could there be a more automatic place where origin is available (envelope.source?)
      var originName =
          envelope.getTags().stream()
              .filter(tag -> tag.startsWith("location:"))
              .findFirst()
              .get()
              .substring("location:".length());

      return entityRef.askWithStatus(
          replyTo ->
              new Drone.UpdateLocation(
                  originName,
                  CoarseGrainedCoordinates.fromProto(envelope.event().getCoordinates()),
                  replyTo),
          askTimeout);
    }
  }
  ;

  public static void initPushedEventsConsumer(ActorSystem<?> system) {
    // Split the slices into N ranges
    var numberOfSliceRanges =
        system
            .settings()
            .config()
            .getInt("restaurant-drone-deliveries-service.drones.projections-slice-count");

    var sliceRanges =
        EventSourcedProvider.sliceRanges(
            system, R2dbcReadJournal.Identifier(), numberOfSliceRanges);

    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            "LocalDronesProjection",
            sliceRanges.size(),
            i -> ProjectionBehavior.create(projection(system, sliceRanges.get(i))),
            ProjectionBehavior.stopMessage());
  }

  private static Projection<EventEnvelope<CoarseDroneLocation>> projection(
      ActorSystem<?> system, Pair<Integer, Integer> sliceRange) {
    var minSlice = sliceRange.first();
    var maxSlice = sliceRange.second();
    var projectionId = ProjectionId.of("DroneEvents", "drone-" + minSlice + "-" + maxSlice);

    SourceProvider<Offset, EventEnvelope<CoarseDroneLocation>> sourceProvider =
        EventSourcedProvider.eventsBySlices(
            system,
            R2dbcReadJournal.Identifier(),
            PRODUCER_ENTITY_TYPE,
            sliceRange.first(),
            sliceRange.second());

    return R2dbcProjection.atLeastOnceAsync(
        projectionId, Optional.empty(), sourceProvider, () -> new LocationHandler(system), system);
  }
}
