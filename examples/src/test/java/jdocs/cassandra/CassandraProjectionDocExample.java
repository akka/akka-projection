/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cassandra;

import java.time.Duration;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.projection.ProjectionContext;
import akka.stream.javadsl.FlowWithContext;
import jdocs.eventsourced.ShoppingCart;

// #daemon-imports
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.projection.ProjectionBehavior;

// #daemon-imports

// #singleton-imports
import akka.cluster.typed.ClusterSingleton;
import akka.cluster.typed.SingletonActor;

// #singleton-imports

// #source-provider-imports
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.Offset;
import akka.projection.javadsl.SourceProvider;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.eventsourced.EventEnvelope;

// #source-provider-imports

// #projection-imports
import akka.projection.cassandra.javadsl.CassandraProjection;
import akka.projection.Projection;
import akka.projection.ProjectionId;

// #projection-imports

// #handler-imports
import akka.projection.javadsl.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

// #handler-imports

// #withRecoveryStrategy
import akka.projection.HandlerRecoveryStrategy;

// #withRecoveryStrategy

// #get-offset
import akka.projection.javadsl.ProjectionManagement;

// #get-offset

// #update-offset
import akka.persistence.query.Sequence;

// #update-offset

public interface CassandraProjectionDocExample {

  // #handler
  public class ShoppingCartHandler extends Handler<EventEnvelope<ShoppingCart.Event>> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public CompletionStage<Done> process(EventEnvelope<ShoppingCart.Event> envelope) {
      ShoppingCart.Event event = envelope.event();
      if (event instanceof ShoppingCart.CheckedOut) {
        ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;
        logger.info(
            "Shopping cart {} was checked out at {}", checkedOut.cartId, checkedOut.eventTime);
        return CompletableFuture.completedFuture(Done.getInstance());
      } else {
        logger.debug("Shopping cart {} changed by {}", event.getCartId(), event);
        return CompletableFuture.completedFuture(Done.getInstance());
      }
    }
  }
  // #handler

  // #grouped-handler
  public class GroupedShoppingCartHandler extends Handler<List<EventEnvelope<ShoppingCart.Event>>> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public CompletionStage<Done> process(List<EventEnvelope<ShoppingCart.Event>> envelopes) {
      envelopes.forEach(
          env -> {
            ShoppingCart.Event event = env.event();
            if (event instanceof ShoppingCart.CheckedOut) {
              ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;
              logger.info(
                  "Shopping cart {} was checked out at {}",
                  checkedOut.cartId,
                  checkedOut.eventTime);
            } else {
              logger.debug("Shopping cart {} changed by {}", event.getCartId(), event);
            }
          });
      return CompletableFuture.completedFuture(Done.getInstance());
    }
  }
  // #grouped-handler

  public static void illustrateAtLeastOnce() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // #sourceProvider
    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
        EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");
    // #sourceProvider

    // #atLeastOnce
    int saveOffsetAfterEnvelopes = 100;
    Duration saveOffsetAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        CassandraProjection.atLeastOnce(
                ProjectionId.of("shopping-carts", "carts-1"),
                sourceProvider,
                () -> new ShoppingCartHandler())
            .withSaveOffset(saveOffsetAfterEnvelopes, saveOffsetAfterDuration);
    // #atLeastOnce

  }

  public static void illustrateAtMostOnce() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
        EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");

    // #atMostOnce
    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        CassandraProjection.atMostOnce(
            ProjectionId.of("shopping-carts", "carts-1"), sourceProvider, ShoppingCartHandler::new);
    // #atMostOnce

  }

  public static void illustrateGrouped() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
        EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");

    // #grouped
    int groupAfterEnvelopes = 20;
    Duration groupAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        CassandraProjection.groupedWithin(
                ProjectionId.of("shopping-carts", "carts-1"),
                sourceProvider,
                GroupedShoppingCartHandler::new)
            .withGroup(groupAfterEnvelopes, groupAfterDuration);
    // #grouped

  }

  public static void illustrateAtLeastOnceFlow() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
        EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");

    // #atLeastOnceFlow

    Logger logger = LoggerFactory.getLogger("example");

    FlowWithContext<
            EventEnvelope<ShoppingCart.Event>, ProjectionContext, Done, ProjectionContext, NotUsed>
        flow =
            FlowWithContext.<EventEnvelope<ShoppingCart.Event>, ProjectionContext>create()
                .map(EventEnvelope::event)
                .map(
                    event -> {
                      if (event instanceof ShoppingCart.CheckedOut) {
                        ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;
                        logger.info(
                            "Shopping cart {} was checked out at {}",
                            checkedOut.cartId,
                            checkedOut.eventTime);
                      } else {
                        logger.debug("Shopping cart {} changed by {}", event.getCartId(), event);
                      }
                      return Done.getInstance();
                    });

    int saveOffsetAfterEnvelopes = 100;
    Duration saveOffsetAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        CassandraProjection.atLeastOnceFlow(
                ProjectionId.of("shopping-carts", "carts-1"), sourceProvider, flow)
            .withSaveOffset(saveOffsetAfterEnvelopes, saveOffsetAfterDuration);
    // #atLeastOnceFlow

  }

  public static void illustrateRecoveryStrategy() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
        EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");

    // #withRecoveryStrategy
    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        CassandraProjection.atLeastOnce(
                ProjectionId.of("shopping-carts", "carts-1"),
                sourceProvider,
                ShoppingCartHandler::new)
            .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(10, Duration.ofSeconds(1)));
    // #withRecoveryStrategy
  }

  public static void illustrateRestart() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
        EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");

    // #withRestartBackoff
    Duration minBackoff = Duration.ofMillis(200);
    Duration maxBackoff = Duration.ofSeconds(5);
    double randomFactor = 0.1;

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        CassandraProjection.atLeastOnce(
                ProjectionId.of("shopping-carts", "carts-1"),
                sourceProvider,
                ShoppingCartHandler::new)
            .withRestartBackoff(minBackoff, maxBackoff, randomFactor);
    // #withRestartBackoff
  }

  static class IllustrateRunningWithShardedDaemon {

    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // #running-source-provider
    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider(String tag) {
      return EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), tag);
    }
    // #running-source-provider

    // #running-projection
    int saveOffsetAfterEnvelopes = 100;
    Duration saveOffsetAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection(String tag) {
      return CassandraProjection.atLeastOnce(
              ProjectionId.of("shopping-carts", tag), sourceProvider(tag), ShoppingCartHandler::new)
          .withSaveOffset(saveOffsetAfterEnvelopes, saveOffsetAfterDuration);
    }
    // #running-projection

    public IllustrateRunningWithShardedDaemon() {
      // #running-with-daemon-process
      ShardedDaemonProcess.get(system)
          .init(
              ProjectionBehavior.Command.class,
              "shopping-carts",
              ShoppingCart.tags.size(),
              id -> ProjectionBehavior.create(projection(ShoppingCart.tags.get((Integer) id))),
              ProjectionBehavior.stopMessage());
      // #running-with-daemon-process
    }
  }

  static class IllustrateRunningWithActor {

    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");
    ActorContext<Void> context = null;

    // #running-with-actor
    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider(String tag) {
      return EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), tag);
    }

    Projection<EventEnvelope<ShoppingCart.Event>> projection(String tag) {
      return CassandraProjection.atLeastOnce(
          ProjectionId.of("shopping-carts", tag), sourceProvider(tag), ShoppingCartHandler::new);
    }

    Projection<EventEnvelope<ShoppingCart.Event>> projection1 = projection("carts-1");

    ActorRef<ProjectionBehavior.Command> projection1Ref =
        context.spawn(ProjectionBehavior.create(projection1), projection1.projectionId().id());
    // #running-with-actor
  }

  static class IllustrateRunningWithSingleton {

    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // #running-with-singleton
    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider(String tag) {
      return EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), tag);
    }

    Projection<EventEnvelope<ShoppingCart.Event>> projection(String tag) {
      return CassandraProjection.atLeastOnce(
          ProjectionId.of("shopping-carts", tag), sourceProvider(tag), ShoppingCartHandler::new);
    }

    Projection<EventEnvelope<ShoppingCart.Event>> projection1 = projection("carts-1");

    ActorRef<ProjectionBehavior.Command> projection1Ref =
        ClusterSingleton.get(system)
            .init(
                SingletonActor.of(
                    ProjectionBehavior.create(projection1), projection1.projectionId().id())
                    .withStopMessage(ProjectionBehavior.stopMessage()));
    // #running-with-singleton
  }

  public static void illustrateProjectionSettings() {

    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
        EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");

    // #projection-settings
    int saveOffsetAfterEnvelopes = 100;
    Duration saveOffsetAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        CassandraProjection.atLeastOnce(
                ProjectionId.of("shopping-carts", "carts-1"),
                sourceProvider,
                ShoppingCartHandler::new)
            .withRestartBackoff(
                Duration.ofSeconds(10), /*minBackoff*/
                Duration.ofSeconds(60), /*maxBackoff*/
                0.5 /*randomFactor*/)
            .withSaveOffset(saveOffsetAfterEnvelopes, saveOffsetAfterDuration);
    // #projection-settings

  }

  public static void illustrateGetOffset() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");
    // #get-offset

    ProjectionId projectionId = ProjectionId.of("shopping-carts", "carts-1");
    CompletionStage<Optional<Offset>> currentOffset =
        ProjectionManagement.get(system).getOffset(projectionId);
    // #get-offset
  }

  public static void illustrateClearOffset() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");
    // #clear-offset

    ProjectionId projectionId = ProjectionId.of("shopping-carts", "carts-1");
    CompletionStage<Done> done = ProjectionManagement.get(system).clearOffset(projectionId);
    // #clear-offset
  }

  public static void illustrateUpdateOffset() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");
    // #update-offset

    ProjectionId projectionId = ProjectionId.of("shopping-carts", "carts-1");
    CompletionStage<Optional<Sequence>> currentOffset =
        ProjectionManagement.get(system).getOffset(projectionId);
    currentOffset.thenAccept(
        optionalOffset -> {
          if (optionalOffset.isPresent()) {
            Sequence newOffset = new Sequence(optionalOffset.get().value());
            CompletionStage<Done> done =
                ProjectionManagement.get(system).updateOffset(projectionId, newOffset);
          }
        });
    // #update-offset
  }

  public static void illustrateIsPaused() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");
    // #is-paused

    ProjectionId projectionId = ProjectionId.of("shopping-carts", "carts-1");
    CompletionStage<Boolean> paused =
        ProjectionManagement.get(system).isPaused(projectionId);
    // #is-paused
  }

  public static void illustratPauseResume() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");
    // #pause-resume

    ProjectionId projectionId = ProjectionId.of("shopping-carts", "carts-1");
    ProjectionManagement mgmt = ProjectionManagement.get(system);
    CompletionStage<Done> pauseDone = mgmt.pause(projectionId);
    CompletionStage<Done> migrationDone = pauseDone.thenCompose(notUsed -> someDataMigration());
    CompletionStage<Done> resumeDone = migrationDone.thenCompose(notUsed -> mgmt.resume(projectionId));
    // #pause-resume
  }

  static CompletionStage<Done> someDataMigration() {
    return CompletableFuture.completedFuture(Done.getInstance());
  }
}
