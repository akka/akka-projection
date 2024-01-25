package iot.temperature;

import akka.Done;
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
import akka.persistence.typed.state.javadsl.CommandHandler;
import akka.persistence.typed.state.javadsl.DurableStateBehavior;
import akka.persistence.typed.state.javadsl.Effect;
import akka.serialization.jackson.CborSerializable;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.Duration;

public class SensorTwin extends DurableStateBehavior<SensorTwin.Command, SensorTwin.State> {

  public interface Command extends CborSerializable {}

  public static class State implements CborSerializable {
    public static final State EMPTY = new State(0);

    public final int temperature;

    @JsonCreator
    public State(int temperature) {
      this.temperature = temperature;
    }
  }

  public static class UpdateTemperature implements Command {
    public final int temperature;
    public final ActorRef<StatusReply<Done>> replyTo;

    public UpdateTemperature(int temperature, ActorRef<StatusReply<Done>> replyTo) {
      this.temperature = temperature;
      this.replyTo = replyTo;
    }
  }

  public static class GetTemperature implements Command {
    public final ActorRef<StatusReply<Integer>> replyTo;

    @JsonCreator
    public GetTemperature(ActorRef<StatusReply<Integer>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static EntityTypeKey<SensorTwin.Command> ENTITY_KEY =
      EntityTypeKey.create(SensorTwin.Command.class, "SensorTwin");

  public static void init(ActorSystem<?> system) {
    ClusterSharding.get(system)
        .init(
            Entity.of(ENTITY_KEY, entityContext -> SensorTwin.create(entityContext.getEntityId())));
  }

  public static Behavior<Command> create(String entityId) {
    return Behaviors.setup(context -> new SensorTwin(entityId));
  }

  private SensorTwin(String entityId) {
    super(
        PersistenceId.of(ENTITY_KEY.name(), entityId),
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.1));
  }

  @Override
  public State emptyState() {
    return State.EMPTY;
  }

  @Override
  public CommandHandler<Command, State> commandHandler() {
    return newCommandHandlerBuilder()
        .forAnyState()
        .onCommand(UpdateTemperature.class, this::handleUpdateTemperature)
        .onCommand(GetTemperature.class, this::handleGetTemperature)
        .build();
  }

  private Effect<State> handleUpdateTemperature(State state, UpdateTemperature command) {
    return Effect()
        .persist(new State(command.temperature))
        .thenReply(command.replyTo, updatedState -> StatusReply.ack());
  }

  private Effect<State> handleGetTemperature(State state, GetTemperature command) {
    return Effect().reply(command.replyTo, StatusReply.success(state.temperature));
  }
}
