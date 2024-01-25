package iot.registration;

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
import akka.persistence.typed.javadsl.CommandHandlerWithReply;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehaviorWithEnforcedReplies;
import akka.persistence.typed.javadsl.ReplyEffect;
import akka.persistence.typed.javadsl.RetentionCriteria;
import akka.serialization.jackson.CborSerializable;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.Duration;

/**
 * This is an event sourced actor (`EventSourcedBehavior`). An entity managed by Cluster Sharding.
 *
 * <p>It has a state, <code>Registration.State</code>, which holds the current secret.
 *
 * <p>You interact with event sourced actors by sending commands to them, see classes implementing
 * <code>Registration.Command</code>.
 *
 * <p>The command handler validates and translates commands to events, see classes implementing
 * <code>Registration.Event</code>. It's the events that are persisted by the
 * `EventSourcedBehavior`. The event handler updates the current state based on the event. This is
 * done when the event is first created, and when the entity is loaded from the database - each
 * event will be replayed to recreate the state of the entity.
 */
public class Registration
    extends EventSourcedBehaviorWithEnforcedReplies<
        Registration.Command, Registration.Event, Registration.State> {

  /** The current state held by the `EventSourcedBehavior`. */
  public static class State implements CborSerializable {
    public static final State EMPTY = new State(new SecretDataValue(""));

    public final SecretDataValue secret;

    @JsonCreator
    public State(SecretDataValue secret) {
      this.secret = secret;
    }

    public State updateSecret(SecretDataValue secret) {
      return new State(secret);
    }
  }

  public static class SecretDataValue implements CborSerializable {
    public final String value;

    @JsonCreator
    public SecretDataValue(String value) {
      this.value = value;
    }
  }

  public interface Command extends CborSerializable {}

  public static class Register implements Command {
    public final SecretDataValue secret;
    public final ActorRef<StatusReply<Done>> replyTo;

    public Register(SecretDataValue secret, ActorRef<StatusReply<Done>> replyTo) {
      this.secret = secret;
      this.replyTo = replyTo;
    }
  }

  public interface Event extends CborSerializable {}

  public static class Registered implements Event {
    public final SecretDataValue secret;

    @JsonCreator
    public Registered(SecretDataValue secret) {
      this.secret = secret;
    }
  }

  public static EntityTypeKey<Command> ENTITY_KEY =
      EntityTypeKey.create(Command.class, "Registration");

  public static void init(ActorSystem<?> system) {
    ClusterSharding.get(system)
        .init(
            Entity.of(
                ENTITY_KEY, entityContext -> Registration.create(entityContext.getEntityId())));
  }

  public static Behavior<Command> create(String entityId) {
    return Behaviors.setup(context -> new Registration(entityId));
  }

  private Registration(String entityId) {
    super(
        PersistenceId.of(ENTITY_KEY.name(), entityId),
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.1));
  }

  @Override
  public State emptyState() {
    return State.EMPTY;
  }

  @Override
  public CommandHandlerWithReply<Command, Event, State> commandHandler() {
    return newCommandHandlerWithReplyBuilder()
        .forAnyState()
        .onCommand(Register.class, this::handleRegister)
        .build();
  }

  private ReplyEffect<Event, State> handleRegister(State state, Register command) {
    return Effect()
        .persist(new Registered(command.secret))
        .thenReply(command.replyTo, updatedState -> StatusReply.ack());
  }

  @Override
  public EventHandler<State, Event> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(Registered.class, (state, event) -> state.updateSecret(event.secret))
        .build();
  }

  @Override
  public RetentionCriteria retentionCriteria() {
    return RetentionCriteria.snapshotEvery(100);
  }
}
