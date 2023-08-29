package local.drones;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.grpc.GrpcClientSettings;
import akka.persistence.Persistence;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.grpc.producer.javadsl.EventProducer;
import akka.projection.grpc.producer.javadsl.EventProducerPush;
import akka.projection.grpc.producer.javadsl.EventProducerSource;
import akka.projection.grpc.producer.javadsl.Transformation;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class DroneEvents {
    private static final Logger logger = LoggerFactory.getLogger(DroneEvents.class);

    public static final String StreamId = "drone-events";

    public static Behavior<ProjectionBehavior.Command> eventToCloudPushBehavior(ActorSystem<?> system,
                                                                                Settings settings) {
        logger.info(
                "Pushing events to central cloud, origin id [{}]",
                settings.locationId);

        // turn events into a public protocol (protobuf) type before publishing
        var eventTransformation =
                Transformation.empty().registerAsyncEnvelopeMapper(Drone.CoarseGrainedLocationChanged.class, (EventEnvelope<Drone.CoarseGrainedLocationChanged> envelope) -> {
            var event = envelope.event();
            return CompletableFuture.completedFuture(
                    Optional.of(local.drones.proto.CoarseDroneLocation.newBuilder().setCoordinates(event.coordinates.toProto()).build()));
        });

        var eventProducer = EventProducerPush.create(
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
                        EventSourcedProvider.eventsBySlices(
                        system,
                                R2dbcReadJournal.Identifier(),
                    eventProducer.eventProducerSource().entityType(),
                0,
                maxSlice),
        eventProducer.handler(system),
                        system));
    }

}
