package local.drones;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.grpc.GrpcClientSettings;
import akka.japi.Pair;
import akka.persistence.Persistence;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.grpc.producer.javadsl.EventProducerPush;
import akka.projection.grpc.producer.javadsl.EventProducerSource;
import akka.projection.grpc.producer.javadsl.Transformation;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroneEvents {
  private static final Logger logger = LoggerFactory.getLogger(DroneEvents.class);

  public static final String StreamId = "drone-events";

  public static Behavior<ProjectionBehavior.Command> eventToCloudPushBehavior(
      ActorSystem<?> system, Settings settings) {
    logger.info("Pushing events to central cloud, origin id [{}]", settings.locationId);

    // turn events into a public protocol (protobuf) type before publishing
    var eventTransformation =
        Transformation.empty()
            .registerAsyncEnvelopeMapper(
                Drone.CoarseGrainedLocationChanged.class,
                (EventEnvelope<Drone.CoarseGrainedLocationChanged> envelope) -> {
                  var event = envelope.event();
                  return CompletableFuture.completedFuture(
                      Optional.of(
                          local.drones.proto.CoarseDroneLocation.newBuilder()
                              .setCoordinates(event.coordinates.toProto())
                              .build()));
                });

    var eventProducer =
        EventProducerPush.create(
            // location id is unique and informative, so use it as producer origin id as well
            settings.locationId,
            new EventProducerSource(
                Drone.ENTITY_KEY.name(),
                StreamId,
                eventTransformation,
                EventProducerSettings.create(system),
                // only push coarse grained coordinate changes
                envelope -> envelope.event() instanceof Drone.CoarseGrainedLocationChanged),
            GrpcClientSettings.fromConfig("central-drone-control", system));

    // For scaling out the local service this would be split up in slices
    // and run across a cluster with sharded daemon process, now it is instead
    // a single projection actor pushing all event slices
    var maxSlice = Persistence.get(system).numberOfSlices() - 1;
    return ProjectionBehavior.create(
        R2dbcProjection.atLeastOnceFlow(
            ProjectionId.of("drone-event-push", "0-" + maxSlice),
            Optional.empty(),
            // #startFromSnapshot
            EventSourcedProvider.eventsBySlicesStartingFromSnapshots(
                system,
                R2dbcReadJournal.Identifier(),
                eventProducer.eventProducerSource().entityType(),
                0,
                maxSlice,
                // start from latest drone snapshot and don't replay history
                (Drone.State state) ->
                    new Drone.CoarseGrainedLocationChanged(state.coarseGrainedCoordinates().get())),
            // #startFromSnapshot
            eventProducer.handler(system),
            system));
  }

  public static void initEventToCloudDaemonProcess(ActorSystem<Void> system, Settings settings) {
    var nrOfEventProducers =
        system.settings().config().getInt("local-drone-control.nr-of-event-producers");
    var sliceRanges = Persistence.get(system).getSliceRanges(nrOfEventProducers);

    // turn events into a public protocol (protobuf) type before publishing
    var eventTransformation =
        Transformation.empty()
            .registerAsyncEnvelopeMapper(
                Drone.CoarseGrainedLocationChanged.class,
                (EventEnvelope<Drone.CoarseGrainedLocationChanged> envelope) -> {
                  var event = envelope.event();
                  return CompletableFuture.completedFuture(
                      Optional.of(
                          local.drones.proto.CoarseDroneLocation.newBuilder()
                              .setCoordinates(event.coordinates.toProto())
                              .build()));
                });

    var eventProducer =
        EventProducerPush.create(
            // location id is unique and informative, so use it as producer origin id as well
            settings.locationId,
            new EventProducerSource(
                Drone.ENTITY_KEY.name(),
                StreamId,
                eventTransformation,
                EventProducerSettings.create(system),
                // only push coarse grained coordinate changes
                envelope -> envelope.event() instanceof Drone.CoarseGrainedLocationChanged),
            GrpcClientSettings.fromConfig("central-drone-control", system));

    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            "drone-event-push",
            nrOfEventProducers,
            idx -> projectionForPartition(system, eventProducer, sliceRanges, idx));
  }

  private static Behavior<ProjectionBehavior.Command> projectionForPartition(
      ActorSystem<?> system,
      EventProducerPush<Object> eventProducer,
      List<Pair<Integer, Integer>> sliceRanges,
      int partition) {
    var sliceRange = sliceRanges.get(partition);
    var minSlice = sliceRange.first();
    var maxSlice = sliceRange.second();

    return ProjectionBehavior.create(
        R2dbcProjection.atLeastOnceFlow(
            ProjectionId.of("drone-event-push", minSlice + "-" + maxSlice),
            Optional.empty(),
            EventSourcedProvider.eventsBySlicesStartingFromSnapshots(
                system,
                R2dbcReadJournal.Identifier(),
                eventProducer.eventProducerSource().entityType(),
                minSlice,
                maxSlice,
                (Drone.State state) ->
                    new Drone.CoarseGrainedLocationChanged(state.coarseGrainedCoordinates().get())),
            eventProducer.handler(system),
            system));
  }
}
