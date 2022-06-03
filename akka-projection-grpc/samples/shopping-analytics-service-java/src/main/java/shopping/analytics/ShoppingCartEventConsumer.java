package shopping.analytics;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.japi.Pair;
import akka.persistence.Persistence;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.consumer.javadsl.GrpcReadJournal;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.proto.CheckedOut;
import shopping.cart.proto.ItemAdded;
import shopping.cart.proto.ItemQuantityAdjusted;
import shopping.cart.proto.ItemRemoved;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.Done.done;

class ShoppingCartEventConsumer {
  private static final Logger log = LoggerFactory.getLogger(ShoppingCartEventConsumer.class);

  static class EventHandler extends Handler<EventEnvelope<Object>> {
    @Override
    public CompletionStage<Done> process(EventEnvelope<Object> envelope) {
      Object event = envelope.getEvent();
      if (event instanceof ItemAdded) {
        ItemAdded itemAdded = (ItemAdded) event;
        log.info("Consumed ItemAdded for cart {}, added {} {}", itemAdded.getCartId(), itemAdded.getQuantity(), itemAdded.getItemId());
      } else if (event instanceof ItemQuantityAdjusted) {
        ItemQuantityAdjusted itemQuantityAdjusted = (ItemQuantityAdjusted) event;
        log.info("Consumed ItemQuantityAdjusted for cart {}, changed {} {}", itemQuantityAdjusted.getCartId(), itemQuantityAdjusted.getQuantity(), itemQuantityAdjusted.getItemId());
      } else if (event instanceof ItemRemoved) {
        ItemRemoved itemRemoved = (ItemRemoved) event;
        log.info("Consumed ItemRemoved for cart {}, removed {}", itemRemoved.getCartId(), itemRemoved.getItemId());
      } else if (event instanceof CheckedOut) {
        CheckedOut checkedOut = (CheckedOut) event;
        log.info("Consumed CheckedOut for cart {}", checkedOut.getCartId());
      } else {
        throw new IllegalArgumentException("Unknown event " + event);
      }
      return CompletableFuture.completedFuture(done());
    }
  }

  public static void init(ActorSystem<?> system) {
    int numberOfProjectionInstances = 4;
    String projectionName = "cart-events";
    List<Pair<Integer, Integer>> sliceRanges = Persistence.get(system).getSliceRanges(numberOfProjectionInstances);
    String entityType = "ShoppingCart";

    ShardedDaemonProcess.get(system).init(
        ProjectionBehavior.Command.class,
        projectionName,
        numberOfProjectionInstances,
        idx -> {
          Pair<Integer, Integer> sliceRange = sliceRanges.get(idx);
          String projectionKey = entityType + "-" + sliceRange.first() + "-" + sliceRange.second();
          ProjectionId projectionId = ProjectionId.of(projectionName, projectionKey);

          SourceProvider<Offset, EventEnvelope<Object>> sourceProvider = EventSourcedProvider.eventsBySlices(
              system,
              GrpcReadJournal.Identifier(),
              entityType,
              sliceRange.first(),
              sliceRange.second());

          return ProjectionBehavior.create(
              R2dbcProjection.atLeastOnceAsync(
                  projectionId,
                  Optional.empty(),
                  sourceProvider,
                  EventHandler::new,
                  system));

        },
        ProjectionBehavior.stopMessage());
  }

}
