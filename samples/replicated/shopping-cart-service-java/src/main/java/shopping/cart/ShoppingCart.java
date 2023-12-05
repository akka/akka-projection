package shopping.cart;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.pattern.StatusReply;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.javadsl.*;
import akka.projection.grpc.replication.javadsl.EdgeReplication;
import akka.projection.grpc.replication.javadsl.ReplicatedBehaviors;
import akka.projection.grpc.replication.javadsl.Replication;
import akka.projection.grpc.replication.javadsl.ReplicationSettings;
import akka.projection.r2dbc.javadsl.R2dbcReplication;
import akka.serialization.jackson.CborSerializable;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
 *
 * <p>This shopping cart is replicated using Replicated Event Sourcing. Multiple entity instances
 * can be active at the same time, so the state must be convergent, and each cart item is modelled
 * as a counter. When checking out the cart, only one of the replicas performs the actual checkout,
 * once it's seen that all replicas have closed this cart which will be after all item updated
 * events have been replicated.
 */
public final class ShoppingCart
    extends EventSourcedBehaviorWithEnforcedReplies<
        ShoppingCart.Command, ShoppingCart.Event, ShoppingCart.State> {

  public static final String ENTITY_TYPE = "replicated-shopping-cart";

  static final String SMALL_QUANTITY_TAG = "small";
  static final String MEDIUM_QUANTITY_TAG = "medium";
  static final String LARGE_QUANTITY_TAG = "large";

  static final String VIP_CUSTOMER_TAG = "vip";

  /** The current state held by the `EventSourcedBehavior`. */
  //#stateUpdateItem
  //#checkoutStep1Event
  //#stateVipCustomer
  static final class State implements CborSerializable {
    final Map<String, Integer> items;
    final Set<ReplicaId> closed;
    private Optional<Instant> checkedOut;
    private boolean vipCustomer = false;

    //#stateUpdateItem
    //#checkoutStep1Event
    //#stateVipCustomer

    public State() {
      this(new HashMap<>(), new HashSet<>(), Optional.empty());
    }


    public State(Map<String, Integer> items, Set<ReplicaId> closed, Optional<Instant> checkedOut) {
      this.items = items;
      this.closed = closed;
      this.checkedOut = checkedOut;
    }

    public boolean isClosed() {
      return !closed.isEmpty();
    }

    //#stateUpdateItem
    public State updateItem(String itemId, int quantity) {
      items.put(itemId, items.getOrDefault(itemId, 0) + quantity);
      return this;
    }
    //#stateUpdateItem

    public State markCustomerVip() {
      vipCustomer = true;
      return this;
    }

    //#checkoutStep1Event
    public State close(ReplicaId replica) {
      closed.add(replica);
      return this;
    }
    //#checkoutStep1Event

    public State checkout(Instant now) {
      checkedOut = Optional.of(now);
      return this;
    }

    public Summary toSummary() {
      return new Summary(items, isClosed());
    }

    public int totalQuantity() {
      return items.values().stream().reduce(0, Integer::sum);
    }

    //#stateVipCustomer
    public Set<String> tags() {
      int total = totalQuantity();
      Set<String> tags = new HashSet<>();
      if (vipCustomer) {
        tags.add(VIP_CUSTOMER_TAG);
      }
      if (total >= 100) {
        tags.add(LARGE_QUANTITY_TAG);
      } else if (total >= 10) {
        tags.add(MEDIUM_QUANTITY_TAG);
      } else {
        tags.add(SMALL_QUANTITY_TAG);
      }
      return tags;
    }
    //#stateVipCustomer
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
    final int quantity;
    final ActorRef<StatusReply<Summary>> replyTo;

    public RemoveItem(String itemId, int quantity, ActorRef<StatusReply<Summary>> replyTo) {
      this.itemId = itemId;
      this.quantity = quantity;
      this.replyTo = replyTo;
    }
  }

  /** A command to check out the shopping cart. */
  public static final class Checkout implements Command {
    final ActorRef<StatusReply<Summary>> replyTo;

    @JsonCreator
    public Checkout(ActorRef<StatusReply<Summary>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  /** Internal command to close a shopping cart that's being checked out. */
  public enum CloseForCheckout implements Command {
    INSTANCE
  }

  /** Internal command to complete the checkout for a shopping cart. */
  public enum CompleteCheckout implements Command {
    INSTANCE
  }

  /** A command to get the current state of the shopping cart. */
  public static final class Get implements Command {
    final ActorRef<Summary> replyTo;

    @JsonCreator
    public Get(ActorRef<Summary> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static final class MarkCustomerVip implements Command {
    final ActorRef<StatusReply<Summary>> replyTo;

    @JsonCreator
    public MarkCustomerVip(ActorRef<StatusReply<Summary>> replyTo) {
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
      this.items.values().removeIf(count -> count <= 0);
      this.checkedOut = checkedOut;
    }
  }

  abstract static class Event implements CborSerializable {}

  // #itemUpdatedEvent
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

  static final class CustomerMarkedVip extends Event {
    final Instant timestamp;

    public CustomerMarkedVip(Instant timestamp) {
      this.timestamp = timestamp;
    }
  }

  static final class Closed extends Event {
    final ReplicaId replica;

    @JsonCreator
    public Closed(ReplicaId replica) {
      this.replica = replica;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Closed that = (Closed) o;
      return Objects.equals(replica, that.replica);
    }

    @Override
    public int hashCode() {
      return Objects.hash(replica);
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

  // #init
  public static Replication<Command> init(ActorSystem<?> system) {
    ReplicationSettings<Command> replicationSettings =
        ReplicationSettings.create(
            Command.class,
            ShoppingCart.ENTITY_TYPE,
            R2dbcReplication.create(system),
            system);
    return Replication.grpcReplication(replicationSettings, ShoppingCart::create, system);
  }

  public static Behavior<Command> create(
      ReplicatedBehaviors<Command, Event, State> replicatedBehaviors) {
    return Behaviors.setup(
        context ->
            replicatedBehaviors.setup(
                replicationContext -> new ShoppingCart(context, replicationContext)));
  }
  // #init

  // Use `initWithProducerFilter` instead of `init` to enable filters based on tags.
  // Add at least a total quantity of 10 to the cart, smaller carts are excluded by the event filter.
  // #init-producerFilter
  public static Replication<Command> initWithProducerFilter(ActorSystem<?> system) {
    Predicate<EventEnvelope<Event>> producerFilter =
        envelope -> envelope.getTags().contains(VIP_CUSTOMER_TAG);
    ReplicationSettings<Command> replicationSettings =
        ReplicationSettings.create(
            Command.class,
            ShoppingCart.ENTITY_TYPE,
            R2dbcReplication.create(system),
            system)
            .withProducerFilter(producerFilter);

    return Replication.grpcReplication(replicationSettings,  ShoppingCart::createWithProducerFilter, system);
  }

  public static Behavior<Command> createWithProducerFilter(
      ReplicatedBehaviors<Command, Event, State> replicatedBehaviors) {
    return Behaviors.setup(
        context ->
            replicatedBehaviors.setup(
                replicationContext -> new ShoppingCart(
                    context,
                    replicationContext,
                    true // onlyReplicateVip flag
                    )));
  }
  // #init-producerFilter

  // #init-allow-edge
  public static Replication<Command> initAllowEdge(ActorSystem<?> system) {
    ReplicationSettings<Command> replicationSettings =
        ReplicationSettings.create(
            Command.class,
            ShoppingCart.ENTITY_TYPE,
            R2dbcReplication.create(system),
            system)
            .withEdgeReplication(true);
    return Replication.grpcReplication(replicationSettings, ShoppingCart::create, system);
  }
  // #init-allow-edge

  // #init-edge
  public static EdgeReplication<Command> initEdge(ActorSystem<?> system) {
    ReplicationSettings<Command> replicationSettings =
        ReplicationSettings.create(
                Command.class,
                ShoppingCart.ENTITY_TYPE,
                R2dbcReplication.create(system),
                system);
    return Replication.grpcEdgeReplication(replicationSettings, ShoppingCart::create, system);
  }
  // #init-edge

  private final ActorContext<Command> context;
  private final ReplicationContext replicationContext;
  private final boolean isLeader;

  private final boolean onlyReplicateVip;

  private ShoppingCart(ActorContext<Command> context, ReplicationContext replicationContext, boolean onlyReplicateVip) {
    super(
        replicationContext.persistenceId(),
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.1));
    this.context = context;
    this.replicationContext = replicationContext;
    this.isLeader = isShoppingCartLeader(replicationContext);
    this.onlyReplicateVip = onlyReplicateVip;
  }

  private ShoppingCart(ActorContext<Command> context, ReplicationContext replicationContext) {
    this(context, replicationContext, false);
  }

  // one replica is responsible for checking out the shopping cart, once all replicas have closed
  // #leader
  private static boolean isShoppingCartLeader(ReplicationContext replicationContext) {
    List<ReplicaId> orderedReplicas =
        replicationContext.getAllReplicas().stream()
            .sorted(Comparator.comparing(ReplicaId::id))
            .collect(Collectors.toList());
    int leaderIndex = Math.abs(replicationContext.entityId().hashCode() % orderedReplicas.size());
    return orderedReplicas.get(leaderIndex) == replicationContext.replicaId();
  }
  // #leader

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
    return openShoppingCart().orElse(closedShoppingCart()).orElse(getCommandHandler()).build();
  }

  private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> openShoppingCart() {
    return newCommandHandlerWithReplyBuilder()
        .forState(state -> !state.isClosed())
        .onCommand(AddItem.class, this::openOnAddItem)
        .onCommand(RemoveItem.class, this::openOnRemoveItem)
        .onCommand(MarkCustomerVip.class, this::openOnMarkCustomerVip)
        .onCommand(Checkout.class, this::openOnCheckout);
  }

  private ReplyEffect<Event, State> openOnAddItem(State state, AddItem cmd) {
    return Effect()
        .persist(new ItemUpdated(cmd.itemId, cmd.quantity))
        .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
  }

  private ReplyEffect<Event, State> openOnRemoveItem(State state, RemoveItem cmd) {
    return Effect()
        .persist(new ItemUpdated(cmd.itemId, -cmd.quantity))
        .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
  }

  private ReplyEffect<Event, State> openOnMarkCustomerVip(State state, MarkCustomerVip cmd) {
    if (!state.vipCustomer) {
      return Effect().persist(new CustomerMarkedVip(Instant.now()))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    } else {
      return Effect().none().thenReply(cmd.replyTo, cart -> StatusReply.success(cart.toSummary()));
    }
  }

  //#checkoutStep1
  private ReplyEffect<Event, State> openOnCheckout(State state, Checkout cmd) {
    return Effect()
        .persist(new Closed(replicationContext.replicaId()))
        .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
  }
  //#checkoutStep1

  private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> closedShoppingCart() {
    return newCommandHandlerWithReplyBuilder()
        .forState(State::isClosed)
        .onCommand(AddItem.class, this::closedOnAddItem)
        .onCommand(RemoveItem.class, this::closedOnRemoveItem)
        .onCommand(MarkCustomerVip.class, this::closedOnMarkCustomerVip)
        .onCommand(Checkout.class, this::closedOnCheckout)
        .onCommand(CloseForCheckout.class, this::closedOnCloseForCheckout)
        .onCommand(CompleteCheckout.class, this::closedOnCompleteCheckout);
  }

  private ReplyEffect<Event, State> closedOnAddItem(State state, AddItem cmd) {
    return Effect()
        .reply(
            cmd.replyTo,
            StatusReply.error("Can't add an item to an already checked out shopping cart"));
  }

  private ReplyEffect<Event, State> closedOnRemoveItem(State state, RemoveItem cmd) {
    return Effect()
        .reply(
            cmd.replyTo,
            StatusReply.error("Can't remove an item from an already checked out shopping cart"));
  }

  private ReplyEffect<Event, State> closedOnMarkCustomerVip(State state, MarkCustomerVip cmd) {
    return Effect()
        .reply(
            cmd.replyTo,
            StatusReply.error("Can't remove an already checked out shopping cart as vip customer"));
  }

  private ReplyEffect<Event, State> closedOnCheckout(State state, Checkout cmd) {
    return Effect()
        .reply(cmd.replyTo, StatusReply.error("Can't checkout already checked out shopping cart"));
  }

  private ReplyEffect<Event, State> closedOnCloseForCheckout(State state, CloseForCheckout cmd) {
    return Effect().persist(new Closed(replicationContext.replicaId())).thenNoReply();
  }

  private ReplyEffect<Event, State> closedOnCompleteCheckout(State state, CompleteCheckout cmd) {
    // TODO: trigger other effects from shopping cart checkout
    return Effect().persist(new CheckedOut(Instant.now())).thenNoReply();
  }

  private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> getCommandHandler() {
    return newCommandHandlerWithReplyBuilder()
        .forAnyState()
        .onCommand(Get.class, (state, cmd) -> Effect().reply(cmd.replyTo, state.toSummary()));
  }

  //#checkoutEventTrigger
  @Override
  public EventHandler<State, Event> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(
            ItemUpdated.class, (state, event) -> state.updateItem(event.itemId, event.quantity))
        .onEvent(
            Closed.class,
            (state, event) -> {
              State newState = state.close(event.replica);
              eventTriggers(newState);
              return newState;
            })
        .onEvent(CheckedOut.class, (state, event) -> state.checkout(event.eventTime))
        .build();
  }

  private void eventTriggers(State state) {
    if (!replicationContext.recoveryRunning()) {
      if (onlyReplicateVip && !state.vipCustomer) {
        // not replicated, no need to coordinate, we can close it right away
        context.getSelf().tell(CompleteCheckout.INSTANCE);
      } else {
        if (!state.closed.contains(replicationContext.replicaId())) {
          context.getSelf().tell(CloseForCheckout.INSTANCE);
        } else if (isLeader) {
          boolean allClosed = replicationContext.getAllReplicas().equals(state.closed);
          if (allClosed) context.getSelf().tell(CompleteCheckout.INSTANCE);
        }
      }
    }
  }
  //#checkoutEventTrigger

  @Override
  public Set<String> tagsFor(State state, Event event) {
    return state.tags();
  }
}
