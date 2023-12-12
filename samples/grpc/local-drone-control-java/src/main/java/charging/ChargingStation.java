package charging;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.pattern.StatusReply;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.javadsl.*;
import akka.projection.grpc.consumer.ConsumerFilter;
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
import java.util.stream.Collectors;

public class ChargingStation
    extends ReplicatedEventSourcedBehavior<
        ChargingStation.Command, ChargingStation.Event, ChargingStation.State> {

  // #commands
  public interface Command extends CborSerializable {}

  public static final class Create implements Command {
    public final String locationId;
    public final int chargingSlots;
    public final ActorRef<StatusReply<Done>> replyTo;

    public Create(String locationId, int chargingSlots, ActorRef<StatusReply<Done>> replyTo) {
      this.locationId = locationId;
      this.chargingSlots = chargingSlots;
      this.replyTo = replyTo;
    }
  }

  public static final class StartCharging implements Command {
    public final String droneId;
    public final ActorRef<StatusReply<StartChargingResponse>> replyTo;

    public StartCharging(String droneId, ActorRef<StatusReply<StartChargingResponse>> replyTo) {
      this.droneId = droneId;
      this.replyTo = replyTo;
    }
  }

  public interface StartChargingResponse extends CborSerializable {}

  public static final class AllSlotsBusy implements StartChargingResponse {
    public final Instant firstSlotFreeAt;

    @JsonCreator
    public AllSlotsBusy(Instant firstSlotFreeAt) {
      this.firstSlotFreeAt = firstSlotFreeAt;
    }
  }

  public static final class GetState implements Command {
    public final ActorRef<StatusReply<State>> replyTo;

    @JsonCreator
    public GetState(ActorRef<StatusReply<State>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static final class CompleteCharging implements Command {
    final String droneId;

    final ActorRef<StatusReply<Done>> replyTo;

    public CompleteCharging(String droneId, ActorRef<StatusReply<Done>> replyTo) {
      this.droneId = droneId;
      this.replyTo = replyTo;
    }
  }
  // #commands

  // #events
  public interface Event extends CborSerializable {}

  public static final class Created implements Event {
    public final String locationId;
    public final int chargingSlots;

    public Created(String locationId, int chargingSlots) {
      this.locationId = locationId;
      this.chargingSlots = chargingSlots;
    }
  }

  public static final class ChargingStarted implements Event, StartChargingResponse {
    public final String droneId;
    public final Instant expectedComplete;

    public ChargingStarted(String droneId, Instant expectedComplete) {
      this.droneId = droneId;
      this.expectedComplete = expectedComplete;
    }
  }

  public static final class ChargingCompleted implements Event {
    public final String droneId;

    @JsonCreator
    public ChargingCompleted(String droneId) {
      this.droneId = droneId;
    }
  }

  public static final class ChargingDrone {
    public final String droneId;
    public final Instant expectedComplete;
    public final String replicaId;

    public ChargingDrone(String droneId, Instant expectedComplete, String replicaId) {
      this.droneId = droneId;
      this.expectedComplete = expectedComplete;
      this.replicaId = replicaId;
    }
  }
  // #events

  // #state
  public static final class State implements CborSerializable {
    public final int chargingSlots;
    public final Set<ChargingDrone> dronesCharging;
    public final String stationLocationId;

    public State(int chargingSlots, Set<ChargingDrone> dronesCharging, String stationLocationId) {
      this.chargingSlots = chargingSlots;
      this.dronesCharging = dronesCharging;
      this.stationLocationId = stationLocationId;
    }
  }
  // #state

  public static final String ENTITY_TYPE = "charging-station";

  private static final Duration FULL_CHARGE_TIME = Duration.ofMinutes(5);

  /**
   * Init for running in edge node, this is the only difference from the ChargingStation in
   * restaurant-deliveries-service
   */
  // #edgeReplicaInit
  public static EdgeReplication<Command> initEdge(ActorSystem<?> system, String locationId) {
    var replicationSettings =
        ReplicationSettings.create(
                Command.class, ENTITY_TYPE, R2dbcReplication.create(system), system)
            .withSelfReplicaId(new ReplicaId(locationId))
            .withInitialConsumerFilter(
                List.of(
                    // only replicate charging stations local to the edge system
                    ConsumerFilter.excludeAll(),
                    new ConsumerFilter.IncludeTopics(Set.of(locationId))));
    return Replication.grpcEdgeReplication(replicationSettings, ChargingStation::create, system);
  }
  // #edgeReplicaInit

  /** Init for running in cloud replica */
  // #replicaInit
  public static Replication<Command> init(ActorSystem<?> system) {
    var replicationSettings =
        ReplicationSettings.create(
                Command.class, ENTITY_TYPE, R2dbcReplication.create(system), system)
            // FIXME remove once release out with flag in config (1.5.1-M2/GA)
            .withEdgeReplication(true);
    return Replication.grpcReplication(replicationSettings, ChargingStation::create, system);
  }
  // #replicaInit

  public static Behavior<Command> create(
      ReplicatedBehaviors<Command, Event, State> replicatedBehaviors) {
    return Behaviors.setup(
        (ActorContext<Command> context) ->
            replicatedBehaviors.setup(
                replicationContext -> {
                  context
                      .getLog()
                      .info("Charging Station {} starting up", replicationContext.entityId());
                  return new ChargingStation(context, replicationContext);
                }));
  }

  private final ActorContext<Command> context;

  public ChargingStation(ActorContext<Command> context, ReplicationContext replicationContext) {
    super(replicationContext);
    this.context = context;
  }

  @Override
  public State emptyState() {
    return null;
  }

  // #commandHandler
  @Override
  public CommandHandler<Command, Event, State> commandHandler() {
    var noStateHandler =
        newCommandHandlerBuilder()
            .forNullState()
            .onCommand(
                Create.class,
                (state, create) ->
                    Effect()
                        .persist(new Created(create.locationId, create.chargingSlots))
                        .thenReply(create.replyTo, stateAfter -> StatusReply.ack()))
            .onCommand(
                StartCharging.class,
                startCharging ->
                    Effect()
                        .reply(
                            startCharging.replyTo,
                            StatusReply.error(
                                "Charging station "
                                    + getReplicationContext().entityId()
                                    + " not initialized")))
            .onCommand(
                GetState.class,
                getState ->
                    Effect()
                        .reply(
                            getState.replyTo,
                            StatusReply.error(
                                "Charging station "
                                    + getReplicationContext().entityId()
                                    + " not initialized")))
            .onCommand(
                command -> true,
                unexpected -> {
                  context
                      .getLog()
                      .warn(
                          "Got an unexpected command {} but charging station with id {} not initialized",
                          unexpected.getClass(),
                          getReplicationContext().entityId());
                  return Effect().none();
                });

    var initializedHandler =
        newCommandHandlerBuilder()
            .forNonNullState()
            .onCommand(
                Create.class,
                create ->
                    Effect()
                        .reply(
                            create.replyTo,
                            StatusReply.error(
                                "Got a create command, but station id "
                                    + getReplicationContext().entityId()
                                    + " was already created")))
            .onCommand(StartCharging.class, this::handleStartCharging)
            .onCommand(CompleteCharging.class, this::handleCompleteCharging)
            .onCommand(
                GetState.class,
                (state, getState) -> Effect().reply(getState.replyTo, StatusReply.success(state)));

    return noStateHandler.orElse(initializedHandler).build();
  }

  private Effect<Event, State> handleStartCharging(State state, StartCharging startCharging) {
    if (state.dronesCharging.stream()
        .anyMatch(charging -> charging.droneId.equals(startCharging.droneId))) {
      return Effect().reply(startCharging.replyTo, StatusReply.error("Drone already charging"));
    } else if (state.dronesCharging.size() >= state.chargingSlots) {
      var earliestFreeSlot =
          state.dronesCharging.stream()
              .min(Comparator.comparing(chargingDrone -> chargingDrone.expectedComplete))
              .get()
              .expectedComplete;
      context
          .getLog()
          .info(
              "Drone {} requested charging but all stations busy, earliest free slot {}",
              startCharging.droneId,
              earliestFreeSlot);
      return Effect()
          .reply(startCharging.replyTo, StatusReply.success(new AllSlotsBusy(earliestFreeSlot)));
    } else {
      // charge
      var expectedComplete = Instant.now().plus(FULL_CHARGE_TIME);
      context
          .getLog()
          .info(
              "Drone {} requested charging, will complete charging at {}",
              startCharging.droneId,
              expectedComplete);
      var event = new ChargingStarted(startCharging.droneId, expectedComplete);
      return Effect()
          .persist(event)
          .thenReply(startCharging.replyTo, newState -> StatusReply.success(event));
    }
  }

  private Effect<Event, State> handleCompleteCharging(
      State state, CompleteCharging completeCharging) {
    context.getLog().info("Drone {} completed charging", completeCharging.droneId);
    if (state.dronesCharging.stream()
        .anyMatch(chargingDrone -> chargingDrone.droneId.equals(completeCharging.droneId)))
      return Effect()
          .persist(new ChargingCompleted(completeCharging.droneId))
          .thenReply(completeCharging.replyTo, newState -> StatusReply.ack());
    else
      return Effect()
          .reply(
              completeCharging.replyTo,
              StatusReply.error(
                  "Drone " + completeCharging.droneId + " is not currently charging."));
  }
  // #commandHandler

  @Override
  public EventHandler<State, Event> eventHandler() {
    var noStateHandler =
        newEventHandlerBuilder()
            .forNullState()
            .onEvent(
                Created.class,
                created ->
                    new State(created.chargingSlots, Collections.emptySet(), created.locationId));

    var initializedStateHandler =
        newEventHandlerBuilder()
            .forNonNullState()
            .onEvent(
                Created.class,
                (state, event) -> {
                  context.getLog().warn("Saw a second created event, ignoring");
                  return state;
                })
            .onEvent(
                ChargingStarted.class,
                (state, event) -> {
                  var newSet = new HashSet<>(state.dronesCharging);
                  newSet.add(
                      new ChargingDrone(
                          event.droneId,
                          event.expectedComplete,
                          getReplicationContext().origin().id()));
                  return new State(
                      state.chargingSlots,
                      Collections.unmodifiableSet(newSet),
                      state.stationLocationId);
                })
            .onEvent(
                ChargingCompleted.class,
                (state, event) -> {
                  var newSet =
                      state.dronesCharging.stream()
                          .filter(charging -> !charging.droneId.equals(event.droneId))
                          .collect(Collectors.toSet());
                  return new State(
                      state.chargingSlots,
                      Collections.unmodifiableSet(newSet),
                      state.stationLocationId);
                });

    return noStateHandler.orElse(initializedStateHandler).build();
  }

  // #tagging
  @Override
  public Set<String> tagsFor(State state, Event event) {
    if (state == null) return Set.of();
    else return Set.of("t:" + state.stationLocationId);
  }
  // #tagging
}
