/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package projection.docs.javadsl;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.serialization.jackson.CborSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// #create-tables
import akka.persistence.dynamodb.util.ClientProvider;
import akka.projection.dynamodb.DynamoDBProjectionSettings;
import akka.projection.dynamodb.javadsl.CreateTables;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

// #create-tables

// #handler
// #grouped-handler
import akka.projection.javadsl.Handler;
// #grouped-handler
// #handler

// #transact-handler
// #grouped-transact-handler
import akka.projection.dynamodb.javadsl.DynamoDBTransactHandler;
// #grouped-transact-handler
// #transact-handler

// #handler
// #transact-handler
// #grouped-handler
// #grouped-transact-handler
import software.amazon.awssdk.services.dynamodb.model.*;

// #grouped-transact-handler
// #grouped-handler
// #transact-handler
// #handler

// #init-projections
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.japi.Pair;
import akka.persistence.dynamodb.query.javadsl.DynamoDBReadJournal;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.Projection;
import akka.projection.ProjectionBehavior;
// #projection-imports
import akka.projection.ProjectionId;
import akka.projection.dynamodb.javadsl.DynamoDBProjection;
// #projection-imports
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.SourceProvider;

import java.util.Optional;

// #projection-imports

// #projection-imports
// #init-projections

public class ProjectionDocExample {

  public static void createTables(ActorSystem<?> system) throws Exception {
    // #create-tables
    String dynamoDBConfigPath = "akka.projection.dynamodb";

    DynamoDBProjectionSettings settings =
        DynamoDBProjectionSettings.create(system.settings().config().getConfig(dynamoDBConfigPath));

    DynamoDbAsyncClient client = ClientProvider.get(system).clientFor(settings.useClient());

    // create journal table, synchronously
    CreateTables.createTimestampOffsetStoreTable(system, settings, client, /*deleteIfExists:*/ true)
        .toCompletableFuture()
        .get(10, TimeUnit.SECONDS);
    // #create-tables
  }

  public static class ShoppingCart {
    public static EntityTypeKey<Command> ENTITY_TYPE_KEY =
        EntityTypeKey.create(Command.class, "ShoppingCart");

    public interface Command extends CborSerializable {}

    public interface Event {
      String getCartId();
    }

    public static class CheckedOut implements Event {

      public final String cartId;
      public final Instant eventTime;

      public CheckedOut(String cartId, Instant eventTime) {
        this.cartId = cartId;
        this.eventTime = eventTime;
      }

      public String getCartId() {
        return cartId;
      }

      @Override
      public String toString() {
        return "CheckedOut(" + cartId + "," + eventTime + ")";
      }
    }
  }

  // #handler
  public class ShoppingCartHandler extends Handler<EventEnvelope<ShoppingCart.Event>> {

    private final DynamoDbAsyncClient client;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public ShoppingCartHandler(DynamoDbAsyncClient client) {
      this.client = client;
    }

    @Override
    public CompletionStage<Done> process(EventEnvelope<ShoppingCart.Event> envelope) {
      ShoppingCart.Event event = envelope.event();
      if (event instanceof ShoppingCart.CheckedOut) {
        ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;

        logger.info(
            "Shopping cart {} was checked out at {}", checkedOut.cartId, checkedOut.eventTime);

        Map<String, AttributeValue> attributes =
            Map.of(
                "id", AttributeValue.fromS(checkedOut.cartId),
                "time", AttributeValue.fromN(String.valueOf(checkedOut.eventTime.toEpochMilli())));

        CompletableFuture<PutItemResponse> response =
            client.putItem(PutItemRequest.builder().tableName("orders").item(attributes).build());

        return response.thenApply(__ -> Done.getInstance());

      } else {
        logger.debug("Shopping cart {} changed by {}", event.getCartId(), event);
        return CompletableFuture.completedFuture(Done.getInstance());
      }
    }
  }

  // #handler

  // #transact-handler
  public class ShoppingCartTransactHandler
      implements DynamoDBTransactHandler<EventEnvelope<ShoppingCart.Event>> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public CompletionStage<Collection<TransactWriteItem>> process(
        EventEnvelope<ShoppingCart.Event> envelope) {
      ShoppingCart.Event event = envelope.event();
      if (event instanceof ShoppingCart.CheckedOut) {
        ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;

        logger.info(
            "Shopping cart {} was checked out at {}", checkedOut.cartId, checkedOut.eventTime);

        Map<String, AttributeValue> attributes =
            Map.of(
                "id", AttributeValue.fromS(checkedOut.cartId),
                "time", AttributeValue.fromN(String.valueOf(checkedOut.eventTime.toEpochMilli())));

        List<TransactWriteItem> items =
            List.of(
                TransactWriteItem.builder()
                    .put(Put.builder().tableName("orders").item(attributes).build())
                    .build());

        return CompletableFuture.completedFuture(items);

      } else {
        logger.debug("Shopping cart {} changed by {}", event.getCartId(), event);
        return CompletableFuture.completedFuture(Collections.emptyList());
      }
    }
  }

  // #transact-handler

  // #grouped-handler
  public class GroupedShoppingCartHandler extends Handler<List<EventEnvelope<ShoppingCart.Event>>> {

    private final DynamoDbAsyncClient client;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public GroupedShoppingCartHandler(DynamoDbAsyncClient client) {
      this.client = client;
    }

    @Override
    public CompletionStage<Done> process(List<EventEnvelope<ShoppingCart.Event>> envelopes) {
      List<WriteRequest> items =
          envelopes.stream()
              .flatMap(
                  envelope -> {
                    ShoppingCart.Event event = envelope.event();

                    if (event instanceof ShoppingCart.CheckedOut) {
                      ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;

                      logger.info(
                          "Shopping cart {} was checked out at {}",
                          checkedOut.cartId,
                          checkedOut.eventTime);

                      Map<String, AttributeValue> attributes =
                          Map.of(
                              "id",
                              AttributeValue.fromS(checkedOut.cartId),
                              "time",
                              AttributeValue.fromN(
                                  String.valueOf(checkedOut.eventTime.toEpochMilli())));

                      return Stream.of(
                          WriteRequest.builder()
                              .putRequest(PutRequest.builder().item(attributes).build())
                              .build());

                    } else {
                      logger.debug("Shopping cart {} changed by {}", event.getCartId(), event);
                      return Stream.empty();
                    }
                  })
              .collect(Collectors.toList());

      CompletableFuture<BatchWriteItemResponse> response =
          client.batchWriteItem(
              BatchWriteItemRequest.builder().requestItems(Map.of("orders", items)).build());

      return response.thenApply(__ -> Done.getInstance());
    }
  }

  // #grouped-handler

  // #grouped-transact-handler
  public class GroupedShoppingCartTransactHandler
      implements DynamoDBTransactHandler<List<EventEnvelope<ShoppingCart.Event>>> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public CompletionStage<Collection<TransactWriteItem>> process(
        List<EventEnvelope<ShoppingCart.Event>> envelopes) {
      List<TransactWriteItem> items =
          envelopes.stream()
              .flatMap(
                  envelope -> {
                    ShoppingCart.Event event = envelope.event();
                    if (event instanceof ShoppingCart.CheckedOut) {
                      ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;

                      logger.info(
                          "Shopping cart {} was checked out at {}",
                          checkedOut.cartId,
                          checkedOut.eventTime);

                      Map<String, AttributeValue> attributes =
                          Map.of(
                              "id", AttributeValue.fromS(checkedOut.cartId),
                              "time",
                                  AttributeValue.fromN(
                                      String.valueOf(checkedOut.eventTime.toEpochMilli())));

                      return Stream.of(
                          TransactWriteItem.builder()
                              .put(Put.builder().tableName("orders").item(attributes).build())
                              .build());

                    } else {
                      logger.debug("Shopping cart {} changed by {}", event.getCartId(), event);
                      return Stream.empty();
                    }
                  })
              .collect(Collectors.toList());

      return CompletableFuture.completedFuture(items);
    }
  }

  // #grouped-transact-handler

  public class InitExample {
    private final ActorSystem<?> system;

    public InitExample(ActorSystem<?> system) {
      this.system = system;
    }

    // #init-projections
    void initProjections() {
      ShardedDaemonProcess.get(system)
          .initWithContext(
              ProjectionBehavior.Command.class,
              "ShoppingCartProjection",
              4,
              daemonContext -> {
                List<Pair<Integer, Integer>> sliceRanges =
                    EventSourcedProvider.sliceRanges(
                        system, DynamoDBReadJournal.Identifier(), daemonContext.totalProcesses());
                Pair<Integer, Integer> sliceRange = sliceRanges.get(daemonContext.processNumber());
                return ProjectionBehavior.create(createProjection(sliceRange));
              },
              ShardedDaemonProcessSettings.create(system),
              Optional.of(ProjectionBehavior.stopMessage()));
    }

    Projection<EventEnvelope<ShoppingCart.Event>> createProjection(
        Pair<Integer, Integer> sliceRange) {
      int minSlice = sliceRange.first();
      int maxSlice = sliceRange.second();

      String entityType = ShoppingCart.ENTITY_TYPE_KEY.name();

      SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
          EventSourcedProvider.eventsBySlices(
              system, DynamoDBReadJournal.Identifier(), entityType, minSlice, maxSlice);

      ProjectionId projectionId =
          ProjectionId.of("ShoppingCarts", "carts-" + minSlice + "-" + maxSlice);
      Optional<DynamoDBProjectionSettings> settings = Optional.empty();

      return DynamoDBProjection.exactlyOnce(
          projectionId, settings, sourceProvider, ShoppingCartTransactHandler::new, system);
    }
    // #init-projections
  }

  public void exactlyOnceExample(
      SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider,
      int minSlice,
      int maxSlice,
      ActorSystem<?> system) {
    // #exactly-once
    ProjectionId projectionId =
        ProjectionId.of("ShoppingCarts", "carts-" + minSlice + "-" + maxSlice);

    Optional<DynamoDBProjectionSettings> settings = Optional.empty();

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        DynamoDBProjection.exactlyOnce(
            projectionId, settings, sourceProvider, ShoppingCartTransactHandler::new, system);
    // #exactly-once
  }

  public void atLeastOnceExample(
      SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider,
      int minSlice,
      int maxSlice,
      DynamoDbAsyncClient client,
      ActorSystem<?> system) {
    // #at-least-once
    ProjectionId projectionId =
        ProjectionId.of("ShoppingCarts", "carts-" + minSlice + "-" + maxSlice);

    Optional<DynamoDBProjectionSettings> settings = Optional.empty();

    int saveOffsetAfterEnvelopes = 100;
    Duration saveOffsetAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        DynamoDBProjection.atLeastOnce(
                projectionId,
                settings,
                sourceProvider,
                () -> new ShoppingCartHandler(client),
                system)
            .withSaveOffset(saveOffsetAfterEnvelopes, saveOffsetAfterDuration);
    // #at-least-once
  }

  public void exactlyOnceGroupedWithinExample(
      SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider,
      int minSlice,
      int maxSlice,
      ActorSystem<?> system) {
    // #exactly-once-grouped-within
    ProjectionId projectionId =
        ProjectionId.of("ShoppingCarts", "carts-" + minSlice + "-" + maxSlice);

    Optional<DynamoDBProjectionSettings> settings = Optional.empty();

    int groupAfterEnvelopes = 20;
    Duration groupAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        DynamoDBProjection.exactlyOnceGroupedWithin(
                projectionId,
                settings,
                sourceProvider,
                GroupedShoppingCartTransactHandler::new,
                system)
            .withGroup(groupAfterEnvelopes, groupAfterDuration);
    // #exactly-once-grouped-within
  }

  public void atLeastOnceGroupedWithinExample(
      SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider,
      int minSlice,
      int maxSlice,
      DynamoDbAsyncClient client,
      ActorSystem<?> system) {
    // #at-least-once-grouped-within
    ProjectionId projectionId =
        ProjectionId.of("ShoppingCarts", "carts-" + minSlice + "-" + maxSlice);

    Optional<DynamoDBProjectionSettings> settings = Optional.empty();

    int groupAfterEnvelopes = 20;
    Duration groupAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        DynamoDBProjection.atLeastOnceGroupedWithin(
                projectionId,
                settings,
                sourceProvider,
                () -> new GroupedShoppingCartHandler(client),
                system)
            .withGroup(groupAfterEnvelopes, groupAfterDuration);
    // #at-least-once-grouped-within
  }

  public void projectionWithSecondPlugin(
      SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider,
      int minSlice,
      int maxSlice,
      DynamoDbAsyncClient client,
      ActorSystem<?> system) {
    // #projection-settings
    ProjectionId projectionId =
        ProjectionId.of("ShoppingCarts", "carts-" + minSlice + "-" + maxSlice);

    Optional<DynamoDBProjectionSettings> settings =
        Optional.of(
            DynamoDBProjectionSettings.create(
                system.settings().config().getConfig("second-projection-dynamodb")));

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        DynamoDBProjection.atLeastOnce(
            projectionId, settings, sourceProvider, () -> new ShoppingCartHandler(client), system);
    // #projection-settings
  }
}
