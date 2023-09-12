package local.drones;

import static akka.actor.typed.javadsl.AskPattern.*;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.grpc.GrpcClientSettings;
import akka.persistence.Persistence;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.consumer.ConsumerFilter;
import akka.projection.grpc.consumer.GrpcQuerySettings;
import akka.projection.grpc.consumer.javadsl.GrpcReadJournal;
import akka.projection.javadsl.Handler;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import central.deliveries.proto.DeliveryRegistered;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

/** Consume delivery events from the cloud and pass to the delivery queue actor */
public class DeliveryEvents {

  public static Behavior<ProjectionBehavior.Command> projectionBehavior(
      ActorSystem<?> system, ActorRef<DeliveriesQueue.Command> queueActor, Settings settings) {
    var projectionName = "delivery-events";

    var eventsBySlicesQuery =
        GrpcReadJournal.create(
            system,
            GrpcQuerySettings.create(system)
                .withInitialConsumerFilter(
                    // location id already is in the format of a topic filter expression
                    Arrays.asList(
                        ConsumerFilter.excludeAll(),
                        new ConsumerFilter.IncludeTopics(
                            Collections.singleton(settings.locationId)))),
            GrpcClientSettings.fromConfig(
                system.settings().config().getConfig("akka.projection.grpc.consumer.client"),
                system),
            Arrays.asList(central.deliveries.proto.DeliveryEvents.getDescriptor()));

    // single projection handling all slices
    var sliceRanges = Persistence.get(system).getSliceRanges(1);
    var sliceRange = sliceRanges.get(0);
    var projectionKey =
        eventsBySlicesQuery.streamId() + "-" + sliceRange.first() + "-" + sliceRange.second();

    var projectionId = ProjectionId.of(projectionName, projectionKey);

    var sourceProvider =
        EventSourcedProvider.<central.deliveries.proto.DeliveryRegistered>eventsBySlices(
            system,
            eventsBySlicesQuery,
            eventsBySlicesQuery.streamId(),
            sliceRange.first(),
            sliceRange.second());

    var handler =
        Handler.fromFunction(
            (EventEnvelope<DeliveryRegistered> envelope) ->
                ask(
                    queueActor,
                    (ActorRef<Done> replyTo) ->
                        new DeliveriesQueue.AddDelivery(
                            new DeliveriesQueue.WaitingDelivery(
                                envelope.event().getDeliveryId(),
                                Coordinates.fromProto(envelope.event().getOrigin()),
                                Coordinates.fromProto(envelope.event().getDestination())),
                            replyTo),
                    settings.askTimeout,
                    system.scheduler()));

    return ProjectionBehavior.create(
        R2dbcProjection.atLeastOnceAsync(
            projectionId, Optional.empty(), sourceProvider, () -> handler, system));
  }
}
