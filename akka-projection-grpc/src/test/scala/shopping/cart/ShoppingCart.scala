/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

// tag::obj[]
package shopping.cart

// end::obj[]

// tag::commands[]
import akka.actor.typed.ActorRef
import akka.pattern.StatusReply

// end::commands[]

// tag::init[]
import scala.concurrent.duration._
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.RetentionCriteria

// end::init[]

// tag::commandHandler[]
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.ReplyEffect

// end::commandHandler[]

// tag::obj[]
object ShoppingCart {

  // end::obj[]

  /**
   * The current state held by the `EventSourcedBehavior`.
   */
  // tag::state[]
  final case class State(items: Map[String, Int]) extends CborSerializable {

    def hasItem(itemId: String): Boolean =
      items.contains(itemId)

    def isEmpty: Boolean =
      items.isEmpty

    def updateItem(itemId: String, quantity: Int): State = {
      quantity match {
        case 0 => copy(items = items - itemId)
        case _ => copy(items = items + (itemId -> quantity))
      }
    }
  }
  object State {
    val empty = State(items = Map.empty)
  }
  // end::state[]

  // tag::commands[]
  /**
   * This interface defines all the commands (messages) that the ShoppingCart actor supports.
   */
  sealed trait Command extends CborSerializable

  /**
   * A command to add an item to the cart.
   *
   * It replies with `StatusReply[Summary]`, which is sent back to the caller when
   * all the events emitted by this command are successfully persisted.
   */
  final case class AddItem(
      itemId: String,
      quantity: Int,
      replyTo: ActorRef[StatusReply[Summary]])
      extends Command

  /**
   * Summary of the shopping cart state, used in reply messages.
   */
  final case class Summary(items: Map[String, Int]) extends CborSerializable
  // end::commands[]

  // tag::events[]
  /**
   * This interface defines all the events that the ShoppingCart supports.
   */
  sealed trait Event extends CborSerializable {
    def cartId: String
  }

  final case class ItemAdded(cartId: String, itemId: String, quantity: Int)
      extends Event
  // end::events[]

  // tag::init[]
  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("ShoppingCart")

  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey) { entityContext =>
      ShoppingCart(entityContext.entityId)
    })
  }

  def apply(cartId: String): Behavior[Command] = {
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        persistenceId = PersistenceId(EntityKey.name, cartId),
        emptyState = State.empty,
        commandHandler =
          (state, command) => handleCommand(cartId, state, command),
        eventHandler = (state, event) => handleEvent(state, event))
      .withRetention(RetentionCriteria
        .snapshotEvery(numberOfEvents = 100, keepNSnapshots = 3))
      .onPersistFailure(
        SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
  }
  // end::init[]

  // tag::commandHandler[]
  private def handleCommand(
      cartId: String,
      state: State,
      command: Command): ReplyEffect[Event, State] = {
    command match {
      case AddItem(itemId, quantity, replyTo) =>
        if (state.hasItem(itemId))
          Effect.reply(replyTo)(
            StatusReply.Error(
              s"Item '$itemId' was already added to this shopping cart"))
        else if (quantity <= 0)
          Effect.reply(replyTo)(
            StatusReply.Error("Quantity must be greater than zero"))
        else
          Effect
            .persist(ItemAdded(cartId, itemId, quantity))
            .thenReply(replyTo) { updatedCart =>
              StatusReply.Success(Summary(updatedCart.items))
            }
    }
  }

  // end::commandHandler[]

  // tag::eventHandler[]
  private def handleEvent(state: State, event: Event) = {
    event match {
      case ItemAdded(_, itemId, quantity) =>
        state.updateItem(itemId, quantity)
    }
  }
  // end::eventHandler[]

// tag::obj[]
}
// end::obj[]
