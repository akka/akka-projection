package shopping.cart;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.pattern.StatusReply;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.*;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * This is an event sourced actor (`EventSourcedBehavior`). An entity managed by Cluster Sharding.
 *
 * <p>It has a state, [[ShoppingCart.State]], which holds the current shopping cart items and
 * whether it's checked out.
 *
 * <p>You interact with event sourced actors by sending commands to them, see classes implementing
 * [[ShoppingCart.Command]].
 *
 * <p>The command handler validates and translates commands to events, see classes implementing
 * [[ShoppingCart.Event]]. It's the events that are persisted by the `EventSourcedBehavior`. The
 * event handler updates the current state based on the event. This is done when the event is first
 * created, and when the entity is loaded from the database - each event will be replayed to
 * recreate the state of the entity.
 */
//#tags
public final class ShoppingCart
    extends EventSourcedBehaviorWithEnforcedReplies<
        ShoppingCart.Command, ShoppingCart.Event, ShoppingCart.State> {

  static final String SMALL_QUANTITY_TAG = "small";
  static final String MEDIUM_QUANTITY_TAG = "medium";
  static final String LARGE_QUANTITY_TAG = "large";

  //#tags

  /** The current state held by the `EventSourcedBehavior`. */
  //#state
  //#tags
  static final class State implements CborSerializable {
    final Map<String, Integer> items;

    //#tags
    private Optional<Instant> checkoutDate;

    public State() {
      this(new HashMap<>(), Optional.empty());
    }

    public State(Map<String, Integer> items, Optional<Instant> checkoutDate) {
      this.items = items;
      this.checkoutDate = checkoutDate;
    }

    public boolean isCheckedOut() {
      return checkoutDate.isPresent();
    }

    public State checkout(Instant now) {
      checkoutDate = Optional.of(now);
      return this;
    }

    public Summary toSummary() {
      return new Summary(items, isCheckedOut());
    }

    public State updateItem(String itemId, int quantity) {
      int newQuantity = items.getOrDefault(itemId, 0) + quantity;
      if (newQuantity > 0)
        items.put(itemId, newQuantity);
      else
        items.remove(itemId);
      return this;
    }

    public boolean isEmpty() {
      return items.isEmpty();
    }

    //#tags
    public int totalQuantity() {
      return items.values().stream().reduce(0, Integer::sum);
    }

    public Set<String> tags() {
      int total = totalQuantity();
      if (total == 0)
        return Collections.emptySet();
      else if (total >= 100)
        return Collections.singleton(LARGE_QUANTITY_TAG);
      else if (total >= 10)
        return Collections.singleton(MEDIUM_QUANTITY_TAG);
      else
        return Collections.singleton(SMALL_QUANTITY_TAG);
    }
  }

  //#state
  //#tags

  //#commands
  /** This interface defines all the commands (messages) that the ShoppingCart actor supports. */
  interface Command extends CborSerializable {}

  /**
   * A command to add an item to the cart.
   *
   * <p>It replies with `StatusReply&lt;Summary&gt;`, which is sent back to the caller when all the
   * events emitted by this command are successfully persisted.
   */
  public static final class AddItem implements Command {
    final String itemId;
    final int quantity;
    final ActorRef<StatusReply<Summary>> replyTo;

    public AddItem(String itemId, int quantity, ActorRef<StatusReply<Summary>> replyTo) {
      this.itemId = itemId;
      this.quantity = quantity;
      this.replyTo = replyTo;
    }
  }

  /** A command to remove an item from the cart. */
  public static final class RemoveItem implements Command {
    final String itemId;
    final int quantity;
    final ActorRef<StatusReply<Summary>> replyTo;

    public RemoveItem(String itemId, int quantity, ActorRef<StatusReply<Summary>> replyTo) {
      this.itemId = itemId;
      this.quantity = quantity;
      this.replyTo = replyTo;
    }
  }

  /** A command to checkout the shopping cart. */
  public static final class Checkout implements Command {
    final ActorRef<StatusReply<Summary>> replyTo;

    @JsonCreator
    public Checkout(ActorRef<StatusReply<Summary>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  /** A command to get the current state of the shopping cart. */
  public static final class Get implements Command {
    final ActorRef<Summary> replyTo;

    @JsonCreator
    public Get(ActorRef<Summary> replyTo) {
      this.replyTo = replyTo;
    }
  }

  /** Summary of the shopping cart state, used in reply messages. */
  public static final class Summary implements CborSerializable {
    final Map<String, Integer> items;
    final boolean checkedOut;

    public Summary(Map<String, Integer> items, boolean checkedOut) {
      // defensive copy since items is a mutable object
      this.items = new HashMap<>(items);
      this.checkedOut = checkedOut;
    }
  }
  //#commands

  //#events
  abstract static class Event implements CborSerializable {
  }

  static final class ItemUpdated extends Event {
    public final String itemId;
    public final int quantity;

    public ItemUpdated(String itemId, int quantity) {
      this.itemId = itemId;
      this.quantity = quantity;
    }
    // #itemUpdatedEvent

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ItemUpdated other = (ItemUpdated) o;

      if (quantity != other.quantity) return false;
      return itemId.equals(other.itemId);
    }

    @Override
    public int hashCode() {
      int result = itemId.hashCode();
      result = 31 * result + quantity;
      return result;
    }
  }

  static final class CheckedOut extends Event {
    final Instant eventTime;

    @JsonCreator
    public CheckedOut(Instant eventTime) {
      this.eventTime = eventTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CheckedOut that = (CheckedOut) o;
      return Objects.equals(eventTime, that.eventTime);
    }

    @Override
    public int hashCode() {
      return Objects.hash(eventTime);
    }
  }
  //#events

  //#init
  static final EntityTypeKey<Command> ENTITY_KEY =
      EntityTypeKey.create(Command.class, "ShoppingCart");

  public static void init(ActorSystem<?> system) {
    ClusterSharding.get(system)
        .init(
            Entity.of(
                ENTITY_KEY,
                entityContext ->
                  ShoppingCart.create(entityContext.getEntityId())
                ));
  }
  //#init

  public static Behavior<Command> create(String cartId) {
    return Behaviors.setup(
        ctx -> EventSourcedBehavior.start(new ShoppingCart(cartId), ctx));
  }

  private final String cartId;

  private ShoppingCart(String cartId) {
    super(
        PersistenceId.of(ENTITY_KEY.name(), cartId),
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.1));
    this.cartId = cartId;
  }

  //#tags
  @Override
  public Set<String> tagsFor(State state, Event event) {
    return state.tags();
  }
  //#tags

  @Override
  public RetentionCriteria retentionCriteria() {
    return RetentionCriteria.snapshotEvery(100, 3);
  }

  @Override
  public State emptyState() {
    return new State();
  }

  @Override
  public CommandHandlerWithReply<Command, Event, State> commandHandler() {
    return openShoppingCart().orElse(checkedOutShoppingCart()).orElse(getCommandHandler()).build();
  }

  //#commandHandler
  private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> openShoppingCart() {
    return newCommandHandlerWithReplyBuilder()
        .forState(state -> !state.isCheckedOut())
        .onCommand(AddItem.class, this::onAddItem)
        .onCommand(RemoveItem.class, this::onRemoveItem)
        .onCommand(Checkout.class, this::onCheckout);
  }
  //#commandHandler

  //#onAddItem
  private ReplyEffect<Event, State> onAddItem(State state, AddItem cmd) {
    if (cmd.quantity <= 0) {
      return Effect().reply(cmd.replyTo, StatusReply.error("Quantity must be greater than zero"));
    } else {
      return Effect()
          .persist(new ItemUpdated(cmd.itemId, cmd.quantity))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    }
  }
  //#onAddItem

  private ReplyEffect<Event, State> onCheckout(State state, Checkout cmd) {
    if (state.isEmpty()) {
      return Effect()
          .reply(cmd.replyTo, StatusReply.error("Cannot checkout an empty shopping cart"));
    } else {
      return Effect()
          .persist(new CheckedOut(Instant.now()))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    }
  }

  private ReplyEffect<Event, State> onRemoveItem(State state, RemoveItem cmd) {
    return Effect()
        .persist(new ItemUpdated(cmd.itemId, -cmd.quantity))
        .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
  }

  private CommandHandlerWithReplyBuilderByState<Command, Event, State, State>
      checkedOutShoppingCart() {
    return newCommandHandlerWithReplyBuilder()
        .forState(State::isCheckedOut)
        .onCommand(
            AddItem.class,
            cmd ->
                Effect()
                    .reply(
                        cmd.replyTo,
                        StatusReply.error(
                            "Can't add an item to an already checked out shopping cart")))
        .onCommand(
            RemoveItem.class,
            cmd ->
                Effect()
                    .reply(
                        cmd.replyTo,
                        StatusReply.error(
                            "Can't remove an item from an already checked out shopping cart")))
        .onCommand(
            Checkout.class,
            cmd ->
                Effect()
                    .reply(
                        cmd.replyTo,
                        StatusReply.error("Can't checkout already checked out shopping cart")));
  }

  private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> getCommandHandler() {
    return newCommandHandlerWithReplyBuilder()
        .forAnyState()
        .onCommand(Get.class, (state, cmd) -> Effect().reply(cmd.replyTo, state.toSummary()));
  }

  //#eventHandler
  @Override
  public EventHandler<State, Event> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(
            ItemUpdated.class,
            (state, evt) -> state.updateItem(evt.itemId, evt.quantity))
        .onEvent(CheckedOut.class, (state, evt) -> state.checkout(evt.eventTime))
        .build();
  }
  //#eventHandler
  //#tags
}
//#tags
