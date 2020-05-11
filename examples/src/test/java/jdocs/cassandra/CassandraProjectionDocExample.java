/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cassandra;

import java.time.Duration;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.Offset;
import akka.projection.eventsourced.EventEnvelope;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.SourceProvider;
import jdocs.eventsourced.ShoppingCart;

//#projection-imports
import akka.projection.cassandra.javadsl.CassandraProjection;
import akka.projection.Projection;
import akka.projection.ProjectionId;

//#projection-imports

//#handler-imports
import akka.projection.javadsl.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

//#handler-imports

public interface CassandraProjectionDocExample {

  //#handler
  public class ShoppingCartHandler extends Handler<EventEnvelope<ShoppingCart.Event>> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public CompletionStage<Done> process(EventEnvelope<ShoppingCart.Event> envelope) {
      ShoppingCart.Event event = envelope.event();
      if (event instanceof ShoppingCart.CheckedOut) {
        ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;
        logger.info("Shopping cart {} was checked out at {}", checkedOut.cartId, checkedOut.eventTime);
        return CompletableFuture.completedFuture(Done.getInstance());
      } else {
        logger.debug("Shopping cart {} changed by {}", event.getCartId(), event);
        return CompletableFuture.completedFuture(Done.getInstance());
      }
    }
  }
  //#handler

  public static void illustrateAtLeastOnce() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    //#sourceProvider
    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
      EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");
    //#sourceProvider

    //#atLeastOnce
    int saveOffsetAfterEnvelopes = 100;
    Duration saveOffsetAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
      CassandraProjection.atLeastOnce(
        ProjectionId.of("ShoppingCarts", "carts-1"),
        sourceProvider,
        saveOffsetAfterEnvelopes,
        saveOffsetAfterDuration,
        new ShoppingCartHandler());
    //#atLeastOnce

  }

  public static void illustrateAtMostOnce() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
      EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");

    //#atMostOnce
    Projection<EventEnvelope<ShoppingCart.Event>> projection =
      CassandraProjection.atMostOnce(
        ProjectionId.of("ShoppingCarts", "carts-1"),
        sourceProvider,
        new ShoppingCartHandler());
    //#atMostOnce

  }
}
