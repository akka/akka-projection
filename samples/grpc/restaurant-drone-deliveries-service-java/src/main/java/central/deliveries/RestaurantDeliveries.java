package central.deliveries;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.pattern.StatusReply;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.Effect;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;
import central.CborSerializable;
import central.Coordinates;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class RestaurantDeliveries
    extends EventSourcedBehavior<
        RestaurantDeliveries.Command, RestaurantDeliveries.Event, RestaurantDeliveries.State> {
  public interface Command extends CborSerializable {}

  public static final class SetUpRestaurant implements Command {
    public final String localControlLocationId;
    public final Coordinates restaurantLocation;
    public final ActorRef<StatusReply<Done>> replyTo;

    public SetUpRestaurant(
        String localControlLocationId,
        Coordinates restaurantLocation,
        ActorRef<StatusReply<Done>> replyTo) {
      this.localControlLocationId = localControlLocationId;
      this.restaurantLocation = restaurantLocation;
      this.replyTo = replyTo;
    }
  }

  public static final class RegisterDelivery implements Command {
    public final String deliveryId;
    public final Coordinates destination;
    public final ActorRef<StatusReply<Done>> replyTo;

    public RegisterDelivery(
        String deliveryId, Coordinates destination, ActorRef<StatusReply<Done>> replyTo) {
      this.deliveryId = deliveryId;
      this.destination = destination;
      this.replyTo = replyTo;
    }
  }

  public static final class ListCurrentDeliveries implements Command {
    public final ActorRef<List<Delivery>> replyTo;

    @JsonCreator
    public ListCurrentDeliveries(ActorRef<List<Delivery>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public interface Event extends CborSerializable {}

  public static final class RestaurantLocationSet implements Event {
    public final String localControlLocationId;
    public final Coordinates coordinates;

    public RestaurantLocationSet(String localControlLocationId, Coordinates coordinates) {
      this.localControlLocationId = localControlLocationId;
      this.coordinates = coordinates;
    }
  }

  public static final class DeliveryRegistered implements Event {
    public final Delivery delivery;

    @JsonCreator
    public DeliveryRegistered(Delivery delivery) {
      this.delivery = delivery;
    }
  }

  public static final class State implements CborSerializable {
    public final String localControlLocationId;
    public final Coordinates restaurantLocation;
    public final List<Delivery> currentDeliveries;

    public State(
        String localControlLocationId,
        Coordinates restaurantLocation,
        List<Delivery> currentDeliveries) {
      this.localControlLocationId = localControlLocationId;
      this.restaurantLocation = restaurantLocation;
      this.currentDeliveries = currentDeliveries;
    }
  }

  public static final class Delivery {
    public final String deliveryId;
    // The following two fields always the same for the same restaurant, so that they can be seen in
    // the downstream projection.
    public final String localControlLocationId;
    public final Coordinates origin;
    public final Coordinates destination;
    public final Instant timestamp;

    public Delivery(
        String deliveryId,
        String localControlLocationId,
        Coordinates origin,
        Coordinates destination,
        Instant timestamp) {
      this.deliveryId = deliveryId;
      this.localControlLocationId = localControlLocationId;
      this.origin = origin;
      this.destination = destination;
      this.timestamp = timestamp;
    }
  }

  public static final EntityTypeKey<Command> ENTITY_KEY =
      EntityTypeKey.create(Command.class, "RestaurantDeliveries");

  public static void init(ActorSystem<?> system) {
    ClusterSharding.get(system)
        .init(
            Entity.of(
                ENTITY_KEY,
                entityContext -> new RestaurantDeliveries(entityContext.getEntityId())));
  }

  @Override
  public Set<String> tagsFor(State state, Event event) {
    if (state == null) {
      return Collections.emptySet();
    } else {
      // tag events with location id as topic, grpc projection filters makes sure only that location
      // picks them up for drone delivery
      return Collections.singleton("t:" + state.localControlLocationId);
    }
  }

  public RestaurantDeliveries(String restaurantId) {
    super(PersistenceId.of(ENTITY_KEY.name(), restaurantId));
  }

  @Override
  public State emptyState() {
    return null;
  }

  @Override
  public CommandHandler<Command, Event, State> commandHandler() {
    var noStateHandler =
        newCommandHandlerBuilder()
            .forNullState()
            .onCommand(SetUpRestaurant.class, this::onSetUpRestaurant)
            .onCommand(
                RegisterDelivery.class,
                command ->
                    Effect()
                        .reply(
                            command.replyTo,
                            StatusReply.error(
                                "Restaurant not yet initialized, cannot accept registrations")))
            .onCommand(
                ListCurrentDeliveries.class,
                command -> Effect().reply(command.replyTo, Collections.emptyList()));

    var stateHandler =
        newCommandHandlerBuilder()
            .forNonNullState()
            .onCommand(
                SetUpRestaurant.class,
                command ->
                    Effect()
                        .reply(
                            command.replyTo,
                            StatusReply.error("Changing restaurant location not supported")))
            .onCommand(RegisterDelivery.class, this::onRegisterDelivery)
            .onCommand(
                ListCurrentDeliveries.class,
                (state, command) ->
                    // reply with defensive copy of internal mutable state
                    Effect().reply(command.replyTo, new ArrayList<>(state.currentDeliveries)));

    return noStateHandler.orElse(stateHandler).build();
  }

  private Effect<Event, State> onRegisterDelivery(State state, RegisterDelivery command) {
    var existing =
        state.currentDeliveries.stream()
            .filter(delivery -> delivery.deliveryId.equals(command.deliveryId))
            .findFirst();

    if (existing.isPresent()) {
      if (existing.get().destination.equals(command.destination)) {
        // already registered
        return Effect().reply(command.replyTo, StatusReply.ack());
      } else {
        return Effect()
            .reply(
                command.replyTo, StatusReply.error("Delivery id exists but for other destination"));
      }
    } else {
      return Effect()
          .persist(
              new DeliveryRegistered(
                  new Delivery(
                      command.deliveryId,
                      state.localControlLocationId,
                      state.restaurantLocation,
                      command.destination,
                      Instant.now())))
          .thenReply(command.replyTo, updatedState -> StatusReply.ack());
    }
  }

  private Effect<Event, State> onSetUpRestaurant(SetUpRestaurant command) {
    return Effect()
        .persist(
            new RestaurantLocationSet(command.localControlLocationId, command.restaurantLocation))
        .thenReply(command.replyTo, updatedState -> StatusReply.ack());
  }

  @Override
  public EventHandler<State, Event> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(RestaurantLocationSet.class, (event) ->
          new State(event.localControlLocationId, event.coordinates, new ArrayList<>())
        )
        .onEvent(DeliveryRegistered.class, (state, event) -> {
          state.currentDeliveries.add(event.delivery);
          return state;
        })
        .build();
  }
}
