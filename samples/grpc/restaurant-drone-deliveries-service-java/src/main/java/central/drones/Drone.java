package central.drones;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.pattern.StatusReply;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.state.javadsl.CommandHandler;
import akka.persistence.typed.state.javadsl.DurableStateBehavior;
import akka.persistence.typed.state.javadsl.Effect;
import akka.serialization.jackson.CborSerializable;
import central.CoarseGrainedCoordinates;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public final class Drone extends DurableStateBehavior<Drone.Command, Drone.State> {

  // #commands
  interface Command extends CborSerializable {}

  public static final class UpdateLocation implements Command {
    public final String locationName;
    public final CoarseGrainedCoordinates coarseGrainedCoordinates;
    public final ActorRef<StatusReply<Done>> replyTo;

    public UpdateLocation(
        String locationName,
        CoarseGrainedCoordinates coarseGrainedCoordinates,
        ActorRef<StatusReply<Done>> replyTo) {
      this.locationName = locationName;
      this.coarseGrainedCoordinates = coarseGrainedCoordinates;
      this.replyTo = replyTo;
    }
  }

  public static final class GetState implements Command {
    public final ActorRef<State> replyTo;

    @JsonCreator
    public GetState(ActorRef<State> replyTo) {
      this.replyTo = replyTo;
    }
  }

  // #commands

  // #state
  public static final class State implements CborSerializable {
    public String locationName;
    public Optional<CoarseGrainedCoordinates> currentLocation;
    public Instant lastChange;

    public State(
        String locationName,
        Optional<CoarseGrainedCoordinates> currentLocation,
        Instant lastChange) {
      this.locationName = locationName;
      this.currentLocation = currentLocation;
      this.lastChange = lastChange;
    }
  }

  // #state

  public static final EntityTypeKey<Command> ENTITY_KEY =
      EntityTypeKey.create(Command.class, "CentralDrone");

  public static void init(ActorSystem<?> system) {
    ClusterSharding.get(system)
        .init(
            Entity.of(
                ENTITY_KEY,
                entityContext ->
                    Behaviors.setup(context -> new Drone(context, entityContext.getEntityId()))));
  }

  private final ActorContext<Command> context;

  private Drone(ActorContext<Command> context, String entityId) {
    super(
        PersistenceId.of(ENTITY_KEY.name(), entityId),
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(100), Duration.ofSeconds(5), 0.1));
    this.context = context;
  }

  // #emptyState
  @Override
  public State emptyState() {
    return new State("unknown", Optional.empty(), Instant.EPOCH);
  }

  // #emptyState

  // #commandHandler
  @Override
  public CommandHandler<Command, State> commandHandler() {
    return newCommandHandlerBuilder()
        .forAnyState()
        .onCommand(UpdateLocation.class, this::onUpdateLocation)
        .onCommand(
            GetState.class,
            (state, command) ->
                // reply with defensive copy since state is mutable
                Effect()
                    .reply(
                        command.replyTo,
                        new State(state.locationName, state.currentLocation, state.lastChange)))
        .build();
  }

  private Effect<State> onUpdateLocation(State state, UpdateLocation command) {
    context
        .getLog()
        .info(
            "Updating location to [{}], [{}]",
            command.locationName,
            command.coarseGrainedCoordinates);
    state.locationName = command.locationName;
    state.currentLocation = Optional.of(command.coarseGrainedCoordinates);
    state.lastChange = Instant.now();
    return Effect().persist(state).thenReply(command.replyTo, updatedState -> StatusReply.ack());
  }
  // #commandHandler
}
