package charging;

import static akka.Done.done;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.persistence.typed.javadsl.*;
import akka.projection.grpc.replication.javadsl.ReplicatedBehaviors;
import akka.projection.grpc.replication.javadsl.Replication;
import akka.projection.grpc.replication.javadsl.ReplicationSettings;
import akka.projection.r2dbc.javadsl.R2dbcReplication;
import akka.serialization.jackson.CborSerializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ChargingStation
    extends ReplicatedEventSourcedBehavior<
        ChargingStation.Command, ChargingStation.Event, ChargingStation.State> {

  // commands and replies
  public interface Command extends CborSerializable {}

  public static final class Create implements Command {
    public final String locationId;
    public final int chargingSlots;
    public final ActorRef<Done> replyTo;

    public Create(String locationId, int chargingSlots, ActorRef<Done> replyTo) {
      this.locationId = locationId;
      this.chargingSlots = chargingSlots;
      this.replyTo = replyTo;
    }
  }

  public static final class StartCharging implements Command {
    public final String droneId;
    public final ActorRef<StartChargingResponse> replyTo;

    public StartCharging(String droneId, ActorRef<StartChargingResponse> replyTo) {
      this.droneId = droneId;
      this.replyTo = replyTo;
    }
  }

  interface StartChargingResponse extends CborSerializable {}

  public static final class AllSlotsBusy implements StartChargingResponse {
    public final Instant firstSlotFreeAt;

    public AllSlotsBusy(Instant firstSlotFreeAt) {
      this.firstSlotFreeAt = firstSlotFreeAt;
    }
  }

  public static final class GetState implements Command {
    public final ActorRef<State> replyTo;

    public GetState(ActorRef<State> replyTo) {
      this.replyTo = replyTo;
    }
  }

  private static final class CompleteCharging implements Command {
    final String droneId;

    public CompleteCharging(String droneId) {
      this.droneId = droneId;
    }
  }

  // events
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
    public final Instant chargeComplete;

    public ChargingStarted(String droneId, Instant chargeComplete) {
      this.droneId = droneId;
      this.chargeComplete = chargeComplete;
    }
  }

  public static final class ChargingCompleted implements Event {
    public final String droneId;

    public ChargingCompleted(String droneId) {
      this.droneId = droneId;
    }
  }

  public static final class ChargingDrone {
    public final String droneId;
    public final Instant chargingDone;
    public final String replicaId;

    public ChargingDrone(String droneId, Instant chargingDone, String replicaId) {
      this.droneId = droneId;
      this.chargingDone = chargingDone;
      this.replicaId = replicaId;
    }
  }

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

  public static final String ENTITY_TYPE = "charging-station";

  private static final Duration FULL_CHARGE_TIME = Duration.ofMinutes(5);

  public static Replication<Command> init(ActorSystem<?> system) {
    var replicationSettings =
        ReplicationSettings.create(
                Command.class, ENTITY_TYPE, R2dbcReplication.create(system), system)
            // FIXME remove once release out with flag in config (1.5.1-M2/GA)
            .withEdgeReplication(true);
    return Replication.grpcReplication(replicationSettings, ChargingStation::create, system);
  }

  public static Behavior<Command> create(
      ReplicatedBehaviors<Command, Event, State> replicatedBehaviors) {
    return Behaviors.setup(
        (ActorContext<Command> context) ->
            Behaviors.withTimers(
                (TimerScheduler<Command> timers) ->
                    replicatedBehaviors.setup(
                        replicationContext -> {
                          context
                              .getLog()
                              .info(
                                  "Charging Station {} starting up", replicationContext.entityId());
                          return new ChargingStation(context, replicationContext, timers);
                        })));
  }

  private static Duration durationUntil(Instant instant) {
    return Duration.ofSeconds(instant.getEpochSecond() - Instant.now().getEpochSecond());
  }

  private final ActorContext<Command> context;
  private final TimerScheduler<Command> timers;

  public ChargingStation(
      ActorContext<Command> context,
      ReplicationContext replicationContext,
      TimerScheduler<Command> timers) {
    super(replicationContext);
    this.context = context;
    this.timers = timers;
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
            .onCommand(
                Create.class,
                (state, create) ->
                    Effect()
                        .persist(new Created(create.locationId, create.chargingSlots))
                        .thenReply(create.replyTo, stateAfter -> done()))
            // FIXME no catch all for particular state in Java?
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
                command -> {
                  context
                      .getLog()
                      .warn(
                          "Got a create command, but station id {} was already created, ignoring",
                          getReplicationContext().entityId());
                  return Effect().none();
                })
            .onCommand(StartCharging.class, this::handleStartCharging)
            .onCommand(
                CompleteCharging.class,
                completeCharging ->
                    Effect().persist(new ChargingCompleted(completeCharging.droneId)))
            .onCommand(
                GetState.class, (state, getState) -> Effect().reply(getState.replyTo, state));

    return noStateHandler.orElse(initializedHandler).build();
  }

  private Effect<Event, State> handleStartCharging(State state, StartCharging startCharging) {
    if (state.dronesCharging.stream()
        .anyMatch(charging -> charging.droneId.equals(startCharging.droneId))) {
      context
          .getLog()
          .warn(
              "Drone {} requested charging but is already charging. Ignoring.",
              startCharging.droneId);
      return Effect().none();
    } else if (state.dronesCharging.size() >= state.chargingSlots) {
      var earliestFreeSlot =
          state.dronesCharging.stream()
              .min(Comparator.comparing(chargingDrone -> chargingDrone.chargingDone))
              .get()
              .chargingDone;
      context
          .getLog()
          .info(
              "Drone {} requested charging but all stations busy, earliest free slot {}",
              startCharging.droneId,
              earliestFreeSlot);
      return Effect().reply(startCharging.replyTo, new AllSlotsBusy(earliestFreeSlot));
    } else {
      // charge
      var chargeCompletedBy = Instant.now().plus(FULL_CHARGE_TIME);
      context
          .getLog()
          .info(
              "Drone {} requested charging, will complete charging at {}",
              startCharging.droneId,
              chargeCompletedBy);
      var event = new ChargingStarted(startCharging.droneId, chargeCompletedBy);
      return Effect()
          .persist(event)
          .thenRun(
              newState -> {
                timers.startSingleTimer(
                    new CompleteCharging(startCharging.droneId), durationUntil(chargeCompletedBy));
                // Note: The event is also the reply
                startCharging.replyTo.tell(event);
              });
    }
  }

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
                          event.chargeComplete,
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
}
