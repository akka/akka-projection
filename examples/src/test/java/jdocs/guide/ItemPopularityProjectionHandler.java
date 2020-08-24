/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.guide;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.projection.eventsourced.EventEnvelope;
import akka.projection.javadsl.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ItemPopularityProjectionHandler extends Handler<EventEnvelope<ShoppingCartEvents.Event>> {
  public final int LogInterval = 10;

  private Logger log = LoggerFactory.getLogger(this.getClass());

  private int logCounter = 0;
  private String tag;
  private ActorSystem<?> system;
  private ItemPopularityProjectionRepository repo;

  public ItemPopularityProjectionHandler(
      String tag, ActorSystem<?> system, ItemPopularityProjectionRepository repo) {
    this.tag = tag;
    this.system = system;
    this.repo = repo;
  }

  /** The Envelope handler to process events. */
  @Override
  public CompletionStage<Done> process(EventEnvelope<ShoppingCartEvents.Event> envelope) throws Exception {
    ShoppingCartEvents.Event event = envelope.event();

    CompletionStage<Done> dbEffect = null;
    if (event instanceof ShoppingCartEvents.ItemAdded) {
      ShoppingCartEvents.ItemAdded added = (ShoppingCartEvents.ItemAdded) event;
      dbEffect = this.repo.update(added.itemId, added.quantity);
    } else if (event instanceof ShoppingCartEvents.ItemQuantityAdjusted) {
      ShoppingCartEvents.ItemQuantityAdjusted adjusted =
          (ShoppingCartEvents.ItemQuantityAdjusted) event;
      dbEffect = this.repo.update(adjusted.itemId, adjusted.newQuantity - adjusted.oldQuantity);
    } else if (event instanceof ShoppingCartEvents.ItemRemoved) {
      ShoppingCartEvents.ItemRemoved removed = (ShoppingCartEvents.ItemRemoved) event;
      dbEffect = this.repo.update(removed.itemId, 0 - removed.oldQuantity);
    } else {
      // skip all other events, such as `CheckedOut`
      dbEffect = CompletableFuture.completedFuture(Done.getInstance());
    }

    dbEffect.thenAccept(done -> logItemCount(event));

    return dbEffect;
  }

  /** Log the popularity of the item in every `ItemEvent` every `LogInterval`. */
  private void logItemCount(ShoppingCartEvents.Event event) {
    if (event instanceof ShoppingCartEvents.ItemEvent) {
      ShoppingCartEvents.ItemEvent itemEvent = (ShoppingCartEvents.ItemEvent) event;
      logCounter += 1;
      if (logCounter == LogInterval) {
        logCounter = 0;
        String itemId = itemEvent.getItemId();
        repo.getItem(itemId)
            .thenAccept(
                opt ->
                    opt.ifPresentOrElse(
                        (count) ->
                            this.log.info(
                                "ItemPopularityProjectionHandler({}) item popularity for '{}': [{}]",
                                this.tag,
                                itemId,
                                count),
                        () ->
                            this.log.info(
                                "ItemPopularityProjectionHandler({}) item popularity for '{}': [0]",
                                this.tag,
                                itemId)));
      }
    }
  }
}
