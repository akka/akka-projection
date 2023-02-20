/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.javadsl;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.grpc.GrpcClientSettings;
import akka.japi.Pair;
import akka.persistence.Persistence;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.consumer.GrpcQuerySettings;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import com.example.shoppingcart.ShoppingcartApiProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static akka.Done.done;

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

    GrpcReadJournal eventsBySlicesQuery = GrpcReadJournal.create(
        system,
        GrpcQuerySettings.create(streamId),
        GrpcClientSettings.fromConfig(
            system.settings().config()
                .getConfig("akka.projection.grpc.consumer.client"), system),
        Collections.singletonList(ShoppingcartApiProto.javaDescriptor()));

    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            projectionName,
            numberOfProjectionInstances,
            idx -> {
              Pair<Integer, Integer> sliceRange = sliceRanges.get(idx);
              String projectionKey =
                  eventsBySlicesQuery.streamId() + "-" + sliceRange.first() + "-" + sliceRange.second();
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
}
