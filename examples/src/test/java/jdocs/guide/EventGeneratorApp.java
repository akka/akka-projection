/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

// #guideEventGeneratorApp
package jdocs.guide;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Join;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Generate a shopping cart every 1 second and check it out. Each cart will contain a variety of
 * `ItemAdded`, `ItemQuantityAdjusted` and `ItemRemoved` events preceding the the cart `Checkout`
 * event.
 */
public class EventGeneratorApp {
  public static void main(String[] args) throws Exception {
    Boolean clusterMode = (args.length > 0 && args[0].equals("cluster"));
    Config config = config();
    ActorSystem<String> system =
        ActorSystem.create(Guardian.create(clusterMode), "EventGeneratorApp", config);
  }

  private static Config config() {
    return ConfigFactory.parseString("akka.actor.provider = cluster")
        .withFallback(ConfigFactory.load("guide-shopping-cart-app.conf"));
  }
}

class Guardian {
  static final List<String> PRODUCTS =
      Arrays.asList("cat t-shirt", "akka t-shirt", "skis", "bowling shoes");

  static final int MAX_QUANTITY = 5;
  static final int MAX_ITEMS = 3;
  static final int MAX_ITEMS_ADJUSTED = 3;

  static final EntityTypeKey<ShoppingCartEvents.Event> ENTITY_KEY =
      EntityTypeKey.create(ShoppingCartEvents.Event.class, "shopping-cart-event");

  static Behavior<String> create(Boolean clusterMode) {

    return Behaviors.setup(
        context -> {
          ActorSystem<Void> system = context.getSystem();
          Cluster cluster = Cluster.get(system);
          cluster.manager().tell(new Join(cluster.selfMember().address()));
          ClusterSharding sharding = ClusterSharding.get(system);

          sharding.init(
              Entity.of(
                  ENTITY_KEY,
                  entityCtx -> {
                    PersistenceId persistenceId = PersistenceId.ofUniqueId(entityCtx.getEntityId());
                    String tag = tagFactory(entityCtx.getEntityId(), clusterMode);
                    return new CartPersistentBehavior(persistenceId, tag);
                  }));

          Source.tick(Duration.ofSeconds(1L), Duration.ofSeconds(1L), "checkout")
              .mapConcat(
                  checkout -> {
                    String cartId = UUID.randomUUID().toString().substring(0, 5);
                    int items = getRandomNumber(1, MAX_ITEMS);
                    Stream<ShoppingCartEvents.ItemEvent> itemEvents =
                        IntStream.range(0, items)
                            // .mapToObj(i -> Integer.valueOf(i)) // Java 8?
                            .boxed()
                            .flatMap(
                                i -> {
                                  String itemId =
                                      String.valueOf(getRandomNumber(0, PRODUCTS.size()));

                                  ArrayList<ShoppingCartEvents.ItemEvent> events =
                                      new ArrayList<>();
                                  // add the item
                                  int quantity = getRandomNumber(1, MAX_QUANTITY);
                                  ShoppingCartEvents.ItemAdded itemAdded =
                                      new ShoppingCartEvents.ItemAdded(cartId, itemId, quantity);

                                  // make up to `MaxItemAdjusted` adjustments to quantity
                                  // of item
                                  int adjustments = getRandomNumber(0, MAX_ITEMS_ADJUSTED);
                                  ArrayList<ShoppingCartEvents.ItemEvent> itemQuantityAdjusted =
                                      new ArrayList<>();
                                  for (int j = 0; j < adjustments; j++) {
                                    int newQuantity = getRandomNumber(1, MAX_QUANTITY);
                                    int oldQuantity = itemAdded.quantity;
                                    if (!itemQuantityAdjusted.isEmpty()) {
                                      oldQuantity =
                                          ((ShoppingCartEvents.ItemQuantityAdjusted)
                                                  itemQuantityAdjusted.get(
                                                      itemQuantityAdjusted.size() - 1))
                                              .newQuantity;
                                    }
                                    itemQuantityAdjusted.add(
                                        new ShoppingCartEvents.ItemQuantityAdjusted(
                                            cartId, itemId, newQuantity, oldQuantity));
                                  }

                                  // flip a coin to decide whether or not to remove the
                                  // item
                                  ArrayList<ShoppingCartEvents.ItemEvent> itemRemoved =
                                      new ArrayList<>();
                                  if (Math.random() % 2 == 0) {
                                    int oldQuantity =
                                        ((ShoppingCartEvents.ItemQuantityAdjusted)
                                                itemQuantityAdjusted.get(
                                                    itemQuantityAdjusted.size() - 1))
                                            .newQuantity;
                                    itemRemoved.add(
                                        new ShoppingCartEvents.ItemRemoved(
                                            cartId, itemId, oldQuantity));
                                  }

                                  events.add(itemAdded);

                                  events.addAll(itemQuantityAdjusted);
                                  events.addAll(itemRemoved);

                                  return events.stream();
                                });

                    // checkout the cart and all its preceding item events
                    return Stream.concat(
                            itemEvents,
                            Stream.of(new ShoppingCartEvents.CheckedOut(cartId, Instant.now())))
                        .collect(Collectors.toList());
                  })
              // send each event to the sharded entity represented by the event's cartId
              .runWith(
                  Sink.foreach(
                      event -> sharding.entityRefFor(ENTITY_KEY, event.getCartId()).tell(event)),
                  system);

          return Behaviors.empty();
        });
  }

  static int getRandomNumber(int min, int max) {
    return (int) ((Math.random() * (max - min)) + min);
  }

  /** Choose a tag from `ShoppingCartTags` based on the entity id (cart id) */
  static String tagFactory(String entityId, Boolean clusterMode) {
    if (clusterMode) {
      int n = Math.abs(entityId.hashCode() % ShoppingCartTags.TAGS.length);
      String selectedTag = ShoppingCartTags.TAGS[n];
      return selectedTag;
    } else return ShoppingCartTags.SINGLE;
  }

  /**
   * An Actor that persists shopping cart events for a particular persistence id (cart id) and tag.
   * This is not how real Event Sourced actors should be be implemented. Please look at
   * https://doc.akka.io/docs/akka/current/typed/persistence.html for more information about
   * `EventSourcedBehavior`.
   */
  static class CartPersistentBehavior
      extends EventSourcedBehavior<
          ShoppingCartEvents.Event, ShoppingCartEvents.Event, List<Object>> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final String tag;
    private final Set<String> tags;

    public CartPersistentBehavior(PersistenceId persistenceId, String tag) {
      super(persistenceId);
      this.tag = tag;
      this.tags = new HashSet<>(Collections.singletonList(tag));
    }

    @Override
    public List<Object> emptyState() {
      return new ArrayList<Object>();
    }

    @Override
    public CommandHandler<ShoppingCartEvents.Event, ShoppingCartEvents.Event, List<Object>>
        commandHandler() {
      return (state, event) -> {
        this.log.info("id [{}] tag [{}] event: {}", this.persistenceId().id(), this.tag, event);
        return Effect().persist(event);
      };
    }

    @Override
    public EventHandler<List<Object>, ShoppingCartEvents.Event> eventHandler() {
      return (state, event) -> state;
    }

    @Override
    public Set<String> tagsFor(ShoppingCartEvents.Event event) {
      return this.tags;
    }
  }
}
// #guideEventGeneratorApp
