package local.drones;

import static akka.Done.done;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.pattern.StatusReply;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.state.javadsl.CommandHandler;
import akka.persistence.typed.state.javadsl.DurableStateBehavior;
import akka.persistence.typed.state.javadsl.Effect;
import akka.serialization.jackson.CborSerializable;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.Instant;
import java.util.*;

public final class DeliveriesQueue
    extends DurableStateBehavior<DeliveriesQueue.Command, DeliveriesQueue.State> {

  // #commands
  public interface Command extends CborSerializable {}

  public static final class AddDelivery implements Command {
    public final WaitingDelivery delivery;
    public final ActorRef<Done> replyTo;

    public AddDelivery(WaitingDelivery delivery, ActorRef<Done> replyTo) {
      this.delivery = delivery;
      this.replyTo = replyTo;
    }
  }

  public static final class RequestDelivery implements Command {
    public final String droneId;
    public final Coordinates droneCoordinates;
    public final ActorRef<StatusReply<WaitingDelivery>> replyTo;

    public RequestDelivery(
        String droneId,
        Coordinates droneCoordinates,
        ActorRef<StatusReply<WaitingDelivery>> replyTo) {
      this.droneId = droneId;
      this.droneCoordinates = droneCoordinates;
      this.replyTo = replyTo;
    }
  }

  public static final class CompleteDelivery implements Command {
    public final String deliveryId;
    public final ActorRef<StatusReply<Done>> replyTo;

    public CompleteDelivery(String deliveryId, ActorRef<StatusReply<Done>> replyTo) {
      this.deliveryId = deliveryId;
      this.replyTo = replyTo;
    }
  }

  public static final class GetCurrentState implements Command {
    public final ActorRef<State> replyTo;

    @JsonCreator
    public GetCurrentState(ActorRef<State> replyTo) {
      this.replyTo = replyTo;
    }
  }

  // #commands

  // #state
  public static final class State implements CborSerializable {
    public final List<WaitingDelivery> waitingDeliveries;
    public final List<DeliveryInProgress> deliveriesInProgress;

    public State() {
      waitingDeliveries = new ArrayList<>();
      deliveriesInProgress = new ArrayList<>();
    }

    public State(
        List<WaitingDelivery> waitingDeliveries, List<DeliveryInProgress> deliveriesInProgress) {
      this.waitingDeliveries = waitingDeliveries;
      this.deliveriesInProgress = deliveriesInProgress;
    }
  }

  public static final class WaitingDelivery {
    public final String deliveryId;
    public final Coordinates from;
    public final Coordinates to;

    public WaitingDelivery(String deliveryId, Coordinates from, Coordinates to) {
      this.deliveryId = deliveryId;
      this.from = from;
      this.to = to;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      WaitingDelivery that = (WaitingDelivery) o;

      if (!deliveryId.equals(that.deliveryId)) return false;
      if (!from.equals(that.from)) return false;
      return to.equals(that.to);
    }

    @Override
    public int hashCode() {
      int result = deliveryId.hashCode();
      result = 31 * result + from.hashCode();
      result = 31 * result + to.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "WaitingDelivery{"
          + "deliveryId='"
          + deliveryId
          + '\''
          + ", from="
          + from
          + ", to="
          + to
          + '}';
    }
  }

  final class DeliveryInProgress {
    public final String deliveryId;
    public final String droneId;
    public final Instant pickupTime;

    public DeliveryInProgress(String deliveryId, String droneId, Instant pickupTime) {
      this.deliveryId = deliveryId;
      this.droneId = droneId;
      this.pickupTime = pickupTime;
    }
  }

  // #state

  // Not really an entity, we just have a single instance
  public static final EntityTypeKey<Command> EntityKey =
      EntityTypeKey.create(Command.class, "RestaurantDeliveries");

  public static Behavior<Command> create() {
    return Behaviors.setup(
        context ->
            new DeliveriesQueue(context, PersistenceId.of(EntityKey.name(), "DeliveriesQueue")));
  }

  private final ActorContext<Command> context;

  public DeliveriesQueue(ActorContext<Command> context, PersistenceId persistenceId) {
    super(persistenceId);
    this.context = context;
  }

  @Override
  public State emptyState() {
    return new State();
  }

  // #commandHandler
  @Override
  public CommandHandler<Command, State> commandHandler() {
    return newCommandHandlerBuilder()
        .forAnyState()
        .onCommand(AddDelivery.class, this::onAddDelivery)
        .onCommand(RequestDelivery.class, this::onRequestDelivery)
        .onCommand(CompleteDelivery.class, this::onCompleteDelivery)
        .onCommand(GetCurrentState.class, this::onGetCurrentState)
        .build();
  }

  private Effect<State> onAddDelivery(State state, AddDelivery command) {
    context.getLog().info("Adding delivery [{}] to queue", command.delivery.deliveryId);
    if (state.waitingDeliveries.contains(command.delivery)
        || state.deliveriesInProgress.stream()
            .anyMatch(
                deliveryInProgress ->
                    deliveryInProgress.deliveryId.equals(command.delivery.deliveryId)))
      return Effect().reply(command.replyTo, done());
    else {
      state.waitingDeliveries.add(command.delivery);
      return Effect().persist(state).thenReply(command.replyTo, updatedState -> done());
    }
  }

  private Effect<State> onRequestDelivery(State state, RequestDelivery command) {
    if (state.waitingDeliveries.isEmpty()) {
      return Effect().reply(command.replyTo, StatusReply.error("No waiting orders"));
    } else {
      var closestPickupForDrone =
          state.waitingDeliveries.stream()
              .min(
                  Comparator.comparingLong(
                      delivery -> command.droneCoordinates.distanceTo(delivery.from)))
              .get();
      context
          .getLog()
          .info(
              "Selected next delivery [{}] for drone [{}]",
              closestPickupForDrone.deliveryId,
              command.droneId);
      // Note: A real application would have to care more about retries/lost data here
      state.waitingDeliveries.remove(closestPickupForDrone);
      state.deliveriesInProgress.add(
          new DeliveryInProgress(closestPickupForDrone.deliveryId, command.droneId, Instant.now()));
      return Effect()
          .persist(state)
          .thenReply(command.replyTo, updatedState -> StatusReply.success(closestPickupForDrone));
    }
  }

  private Effect<State> onCompleteDelivery(State state, CompleteDelivery command) {
    var maybeExisting =
        state.deliveriesInProgress.stream()
            .filter(delivery -> delivery.deliveryId.equals(command.deliveryId))
            .findFirst();
    if (maybeExisting.isEmpty()) {
      return Effect()
          .reply(command.replyTo, StatusReply.error("Unknown delivery id: " + command.deliveryId));
    } else {
      var existing = maybeExisting.get();
      state.deliveriesInProgress.remove(existing);

      return Effect()
          .persist(state)
          .thenReply(command.replyTo, updatedState -> StatusReply.success(done()));
    }
  }

  private Effect<State> onGetCurrentState(State state, GetCurrentState command) {
    // defensive copy since state is mutable (individual values in the lists are immutable)
    var stateToShare =
        new State(
            new ArrayList<>(state.waitingDeliveries), new ArrayList<>(state.deliveriesInProgress));
    return Effect().reply(command.replyTo, stateToShare);
  }
  // #commandHandler
}
