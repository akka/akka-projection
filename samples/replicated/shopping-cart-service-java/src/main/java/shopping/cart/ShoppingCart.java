package shopping.cart;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.pattern.StatusReply;
import akka.persistence.typed.javadsl.*;
import akka.projection.grpc.replication.javadsl.ReplicatedBehaviors;
import akka.projection.grpc.replication.javadsl.Replication;
import akka.projection.grpc.replication.javadsl.ReplicationProjectionProvider;
import akka.projection.grpc.replication.javadsl.ReplicationSettings;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
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
public final class ShoppingCart
    extends EventSourcedBehaviorWithEnforcedReplies<
        ShoppingCart.Command, ShoppingCart.Event, ShoppingCart.State> {

  /** The current state held by the `EventSourcedBehavior`. */
  static final class State implements CborSerializable {
    final Map<String, Integer> items;
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

    public boolean hasItem(String itemId) {
      return items.containsKey(itemId);
    }

    public State updateItem(String itemId, int quantity) {
      if (quantity == 0) {
        items.remove(itemId);
      } else {
        items.put(itemId, quantity);
      }
      return this;
    }

    public boolean isEmpty() {
      return items.isEmpty();
    }

    public State removeItem(String itemId) {
      items.remove(itemId);
      return this;
    }

    public int itemCount(String itemId) {
      return items.get(itemId);
    }
  }

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
    final ActorRef<StatusReply<Summary>> replyTo;

    public RemoveItem(String itemId, ActorRef<StatusReply<Summary>> replyTo) {
      this.itemId = itemId;
      this.replyTo = replyTo;
    }
  }

  /** A command to adjust the quantity of an item in the cart. */
  public static final class AdjustItemQuantity implements Command {
    final String itemId;
    final int quantity;
    final ActorRef<StatusReply<Summary>> replyTo;

    public AdjustItemQuantity(String itemId, int quantity, ActorRef<StatusReply<Summary>> replyTo) {
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

  abstract static class Event implements CborSerializable {
    public final String cartId;

    public Event(String cartId) {
      this.cartId = cartId;
    }
  }

  static final class ItemAdded extends Event {
    public final String itemId;
    public final int quantity;

    public ItemAdded(String cartId, String itemId, int quantity) {
      super(cartId);
      this.itemId = itemId;
      this.quantity = quantity;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ItemAdded other = (ItemAdded) o;

      if (quantity != other.quantity) return false;
      if (!cartId.equals(other.cartId)) return false;
      return itemId.equals(other.itemId);
    }

    @Override
    public int hashCode() {
      int result = cartId.hashCode();
      result = 31 * result + itemId.hashCode();
      result = 31 * result + quantity;
      return result;
    }
  }

  static final class ItemRemoved extends Event {
    public final String itemId;
    public final int oldQuantity;

    public ItemRemoved(String cartId, String itemId, int oldQuantity) {
      super(cartId);
      this.itemId = itemId;
      this.oldQuantity = oldQuantity;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ItemRemoved other = (ItemRemoved) o;

      if (oldQuantity != other.oldQuantity) return false;
      if (!cartId.equals(other.cartId)) return false;
      return itemId.equals(other.itemId);
    }

    @Override
    public int hashCode() {
      int result = cartId.hashCode();
      result = 31 * result + itemId.hashCode();
      result = 31 * result + oldQuantity;
      return result;
    }
  }

  static final class ItemQuantityAdjusted extends Event {
    public final String itemId;
    final int oldQuantity;
    final int newQuantity;

    public ItemQuantityAdjusted(String cartId, String itemId, int oldQuantity, int newQuantity) {
      super(cartId);
      this.itemId = itemId;
      this.oldQuantity = oldQuantity;
      this.newQuantity = newQuantity;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ItemQuantityAdjusted other = (ItemQuantityAdjusted) o;

      if (oldQuantity != other.oldQuantity) return false;
      if (newQuantity != other.newQuantity) return false;
      if (!cartId.equals(other.cartId)) return false;
      return itemId.equals(other.itemId);
    }

    @Override
    public int hashCode() {
      int result = cartId.hashCode();
      result = 31 * result + itemId.hashCode();
      result = 31 * result + oldQuantity;
      result = 31 * result + newQuantity;
      return result;
    }
  }

  static final class CheckedOut extends Event {
    final Instant eventTime;

    public CheckedOut(String cartId, Instant eventTime) {
      super(cartId);
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

  public static Replication<Command> init(ActorSystem<?> system) {
    ReplicationProjectionProvider projectionProvider =
        (projectionId, sourceProvider, replicationFlow, actorSystem) ->
            R2dbcProjection.atLeastOnceFlow(
                projectionId, Optional.empty(), sourceProvider, replicationFlow, actorSystem);
    ReplicationSettings<Command> replicationSettings =
        ReplicationSettings.create(
            Command.class, "replicated-shopping-cart", projectionProvider, system);
    return Replication.grpcReplication(replicationSettings, ShoppingCart::create, system);
  }

  public static Behavior<Command> create(ReplicatedBehaviors<Command, Event, State> replicatedBehaviors) {
    return replicatedBehaviors.setup(ShoppingCart::new);
  }

  private final String cartId;

  private ShoppingCart(ReplicationContext replicationContext) {
    super(
        replicationContext.persistenceId(),
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.1));
    this.cartId = replicationContext.entityId();
  }

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

  private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> openShoppingCart() {
    return newCommandHandlerWithReplyBuilder()
        .forState(state -> !state.isCheckedOut())
        .onCommand(AddItem.class, this::onAddItem)
        .onCommand(RemoveItem.class, this::onRemoveItem)
        .onCommand(AdjustItemQuantity.class, this::onAdjustItemQuantity)
        .onCommand(Checkout.class, this::onCheckout);
  }

  private ReplyEffect<Event, State> onAddItem(State state, AddItem cmd) {
    if (state.hasItem(cmd.itemId)) {
      return Effect()
          .reply(
              cmd.replyTo,
              StatusReply.error(
                  "Item '" + cmd.itemId + "' was already added to this shopping cart"));
    } else if (cmd.quantity <= 0) {
      return Effect().reply(cmd.replyTo, StatusReply.error("Quantity must be greater than zero"));
    } else {
      return Effect()
          .persist(new ItemAdded(cartId, cmd.itemId, cmd.quantity))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    }
  }

  private ReplyEffect<Event, State> onCheckout(State state, Checkout cmd) {
    if (state.isEmpty()) {
      return Effect()
          .reply(cmd.replyTo, StatusReply.error("Cannot checkout an empty shopping cart"));
    } else {
      return Effect()
          .persist(new CheckedOut(cartId, Instant.now()))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    }
  }

  private ReplyEffect<Event, State> onRemoveItem(State state, RemoveItem cmd) {
    if (state.hasItem(cmd.itemId)) {
      return Effect()
          .persist(new ItemRemoved(cartId, cmd.itemId, state.itemCount(cmd.itemId)))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    } else {
      return Effect()
          .reply(
              cmd.replyTo,
              StatusReply.success(state.toSummary())); // removing an item is idempotent
    }
  }

  private ReplyEffect<Event, State> onAdjustItemQuantity(State state, AdjustItemQuantity cmd) {
    if (cmd.quantity <= 0) {
      return Effect().reply(cmd.replyTo, StatusReply.error("Quantity must be greater than zero"));
    } else if (state.hasItem(cmd.itemId)) {
      return Effect()
          .persist(
              new ItemQuantityAdjusted(
                  cartId, cmd.itemId, state.itemCount(cmd.itemId), cmd.quantity))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    } else {
      return Effect()
          .reply(
              cmd.replyTo,
              StatusReply.error(
                  "Cannot adjust quantity for item '"
                      + cmd.itemId
                      + "'. Item not present on cart"));
    }
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
            AdjustItemQuantity.class,
            cmd ->
                Effect()
                    .reply(
                        cmd.replyTo,
                        StatusReply.error(
                            "Can't adjust item on an already checked out shopping cart")))
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

  @Override
  public EventHandler<State, Event> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(ItemAdded.class, (state, evt) -> state.updateItem(evt.itemId, evt.quantity))
        .onEvent(ItemRemoved.class, (state, evt) -> state.removeItem(evt.itemId))
        .onEvent(
            ItemQuantityAdjusted.class,
            (state, evt) -> state.updateItem(evt.itemId, evt.newQuantity))
        .onEvent(CheckedOut.class, (state, evt) -> state.checkout(evt.eventTime))
        .build();
  }
}
