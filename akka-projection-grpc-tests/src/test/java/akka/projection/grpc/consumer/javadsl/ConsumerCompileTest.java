/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer.javadsl;

import static akka.Done.done;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.grpc.GrpcClientSettings;
import akka.japi.Pair;
import akka.persistence.Persistence;
import akka.persistence.query.Offset;
import akka.persistence.query.TimestampOffset;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.consumer.ConsumerFilter;
import akka.projection.grpc.consumer.GrpcQuerySettings;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import com.example.shoppingcart.ShoppingcartApiProto;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerCompileTest {
  static class EventHandler extends Handler<EventEnvelope<String>> {
    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public CompletionStage<Done> process(EventEnvelope<String> envelope) {
      log.info("Consumed event: {}", envelope);
      return CompletableFuture.completedFuture(done());
    }
  }

  public static void init(ActorSystem<?> system) {
    int numberOfProjectionInstances = 1;
    String projectionName = "cart-events";
    List<Pair<Integer, Integer>> sliceRanges =
        Persistence.get(system).getSliceRanges(numberOfProjectionInstances);
    String streamId = "ShoppingCart";

    GrpcReadJournal eventsBySlicesQuery =
        GrpcReadJournal.create(
            system,
            GrpcQuerySettings.create(streamId),
            GrpcClientSettings.fromConfig(
                system.settings().config().getConfig("akka.projection.grpc.consumer.client"),
                system),
            Collections.singletonList(ShoppingcartApiProto.javaDescriptor()));

    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            projectionName,
            numberOfProjectionInstances,
            idx -> {
              Pair<Integer, Integer> sliceRange = sliceRanges.get(idx);
              String projectionKey =
                  eventsBySlicesQuery.streamId()
                      + "-"
                      + sliceRange.first()
                      + "-"
                      + sliceRange.second();
              ProjectionId projectionId = ProjectionId.of(projectionName, projectionKey);

              SourceProvider<Offset, EventEnvelope<String>> sourceProvider =
                  EventSourcedProvider.eventsBySlices(
                      system,
                      eventsBySlicesQuery,
                      eventsBySlicesQuery.streamId(),
                      sliceRange.first(),
                      sliceRange.second());

              return ProjectionBehavior.create(
                  R2dbcProjection.atLeastOnceAsync(
                      projectionId, Optional.empty(), sourceProvider, EventHandler::new, system));
            },
            ProjectionBehavior.stopMessage());
  }

  static void updateConsumerFilter(
      ActorSystem<?> system, Set<String> excludeTags, Set<String> includeTags) {
    String streamId =
        system.settings().config().getString("akka.projection.grpc.consumer.stream-id");

    List<ConsumerFilter.FilterCriteria> criteria =
        Arrays.asList(
            new ConsumerFilter.ExcludeTags(excludeTags),
            new ConsumerFilter.IncludeTags(includeTags));

    ConsumerFilter.get(system).ref().tell(new ConsumerFilter.UpdateFilter(streamId, criteria));
  }

  public static void customStartOffset(ActorSystem<?> system) {
    int numberOfProjectionInstances = 1;
    String projectionName = "cart-events";
    List<Pair<Integer, Integer>> sliceRanges =
        Persistence.get(system).getSliceRanges(numberOfProjectionInstances);
    String streamId = "ShoppingCart";
    int idx = 0;

    GrpcReadJournal eventsBySlicesQuery =
        GrpcReadJournal.create(
            system, Collections.singletonList(ShoppingcartApiProto.javaDescriptor()));

    Pair<Integer, Integer> sliceRange = sliceRanges.get(idx);

    // #adjustStartOffset
    SourceProvider<Offset, EventEnvelope<String>> sourceProvider =
        EventSourcedProvider.eventsBySlices(
            system,
            eventsBySlicesQuery,
            eventsBySlicesQuery.streamId(),
            sliceRange.first(),
            sliceRange.second(),
            (Optional<Offset> storedOffset) -> {
              TimestampOffset startOffset = Offset.timestamp(Instant.now().minusSeconds(3600));
              return CompletableFuture.completedFuture(Optional.of(startOffset));
            });
    // #adjustStartOffset

  }
}
