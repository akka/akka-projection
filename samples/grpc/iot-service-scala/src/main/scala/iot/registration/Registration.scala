package iot.registration

import scala.concurrent.duration._

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplyEffect
import akka.persistence.typed.scaladsl.RetentionCriteria
import akka.serialization.jackson.CborSerializable

/**
 * This is an event sourced actor (`EventSourcedBehavior`). An entity managed by Cluster Sharding.
 *
 * It has a state, [[Registration.State]], which holds the current secret.
 *
 * You interact with event sourced actors by sending commands to them,
 * see classes implementing [[Registration.Command]].
 *
 * The command handler validates and translates commands to events, see classes implementing [[Registration.Event]].
 * It's the events that are persisted by the `EventSourcedBehavior`. The event handler updates the current
 * state based on the event. This is done when the event is first created, and when the entity is
 * loaded from the database - each event will be replayed to recreate the state
 * of the entity.
 */
object Registration {

  /**
   * The current state held by the `EventSourcedBehavior`.
   */
  final case class State(secret: SecretDataValue) extends CborSerializable {

    def updateSecret(secret: SecretDataValue): State = {
      copy(secret = secret)
    }

  }

  object State {
    val empty: State =
      State(secret = SecretDataValue(""))
  }

  final case class SecretDataValue(value: String) extends CborSerializable

  sealed trait Command extends CborSerializable

  final case class Register(
      secret: SecretDataValue,
      replyTo: ActorRef[StatusReply[Done]])
      extends Command

  sealed trait Event extends CborSerializable

  final case class Registered(secret: SecretDataValue)
      extends Event

  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("Registration")

  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey)(entityContext =>
      Registration(entityContext.entityId)))
  }

  def apply(entityId: String): Behavior[Command] = {
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        persistenceId = PersistenceId(EntityKey.name, entityId),
        emptyState = State.empty,
        commandHandler =
          (state, command) => handleCommand(state, command),
        eventHandler = (state, event) => handleEvent(state, event))
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100))
      .onPersistFailure(
        SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
  }

  private def handleCommand(
      state: State,
      command: Command): ReplyEffect[Event, State] = {
    command match {
      case Register(secret, replyTo) =>
        Effect.persist(Registered(secret)).thenReply(replyTo) { _ =>
          StatusReply.Ack
        }
    }
  }

  private def handleEvent(state: State, event: Event): State = {
    event match {
      case Registered(secret) =>
        state.updateSecret(secret)
    }
  }

}
