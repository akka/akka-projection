/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.eventsourced

import java.time.Instant

import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplyEffect
import akka.persistence.typed.scaladsl.RetentionCriteria
import akka.serialization.jackson.CborSerializable

//#imports
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey

//#imports

/**
 * This is an event sourced actor. It has a state, [[ShoppingCart.State]], which
 * stores the current shopping cart items and whether it's checked out.
 *
 * Event sourced actors are interacted with by sending them commands,
 * see classes implementing [[ShoppingCart.Command]].
 *
 * Commands get translated to events, see classes implementing [[ShoppingCart.Event]].
 * It's the events that get persisted by the entity. Each event will have an event handler
 * registered for it, and an event handler updates the current state based on the event.
 * This will be done when the event is first created, and it will also be done when the entity is
 * loaded from the database - each event will be replayed to recreate the state
 * of the entity.
 */
object ShoppingCart {

  /**
   * The current state held by the persistent entity.
   */
  final case class State(items: Map[String, Int], checkoutDate: Option[Instant]) extends CborSerializable {

    def isCheckedOut: Boolean =
      checkoutDate.isDefined

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

    def removeItem(itemId: String): State =
      copy(items = items - itemId)

    def checkout(now: Instant): State =
      copy(checkoutDate = Some(now))

    def toSummary: Summary =
      Summary(items, isCheckedOut)
  }
  object State {
    val empty = State(items = Map.empty, checkoutDate = None)
  }

  /**
   * This interface defines all the commands that the ShoppingCart persistent actor supports.
   */
  sealed trait Command extends CborSerializable

  /**
   * A command to add an item to the cart.
   *
   * It can reply with `Confirmation`, which is sent back to the caller when
   * all the events emitted by this command are successfully persisted.
   */
  final case class AddItem(itemId: String, quantity: Int, replyTo: ActorRef[Confirmation]) extends Command

  /**
   * A command to remove an item from the cart.
   */
  final case class RemoveItem(itemId: String, replyTo: ActorRef[Confirmation]) extends Command

  /**
   * A command to adjust the quantity of an item in the cart.
   */
  final case class AdjustItemQuantity(itemId: String, quantity: Int, replyTo: ActorRef[Confirmation]) extends Command

  /**
   * A command to checkout the shopping cart.
   */
  final case class Checkout(replyTo: ActorRef[Confirmation]) extends Command

  /**
   * A command to get the current state of the shopping cart.
   */
  final case class Get(replyTo: ActorRef[Summary]) extends Command

  /**
   * Summary of the shopping cart state, used in reply messages.
   */
  final case class Summary(items: Map[String, Int], checkedOut: Boolean) extends CborSerializable

  sealed trait Confirmation extends CborSerializable

  final case class Accepted(summary: Summary) extends Confirmation

  final case class Rejected(reason: String) extends Confirmation

  /**
   * This interface defines all the events that the ShoppingCart supports.
   */
  sealed trait Event extends CborSerializable {
    def cartId: String
  }

  final case class ItemAdded(cartId: String, itemId: String, quantity: Int) extends Event

  final case class ItemRemoved(cartId: String, itemId: String) extends Event

  final case class ItemQuantityAdjusted(cartId: String, itemId: String, newQuantity: Int) extends Event

  final case class CheckedOut(cartId: String, eventTime: Instant) extends Event

  //#slicingTags
  val tags = Vector.tabulate(5)(i => s"carts-$i")
  //#slicingTags

  //#tagging
  val EntityKey: EntityTypeKey[Command] = EntityTypeKey[Command]("ShoppingCart")

  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey) { entityContext =>
      val i = math.abs(entityContext.entityId.hashCode % tags.size)
      val selectedTag = tags(i)
      ShoppingCart(entityContext.entityId, selectedTag)
    }.withRole("write-model"))
  }

  def apply(cartId: String, projectionTag: String): Behavior[Command] = {
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        PersistenceId(EntityKey.name, cartId),
        State.empty,
        (state, command) =>
          //The shopping cart behavior changes if it's checked out or not.
          // The commands are handled differently for each case.
          if (state.isCheckedOut) checkedOutShoppingCart(cartId, state, command)
          else openShoppingCart(cartId, state, command),
        (state, event) => handleEvent(state, event))
      .withTagger(_ => Set(projectionTag))
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100))
      .onPersistFailure(SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
  }
  //#tagging

  private def openShoppingCart(cartId: String, state: State, command: Command): ReplyEffect[Event, State] =
    command match {
      case AddItem(itemId, quantity, replyTo) =>
        if (state.hasItem(itemId))
          Effect.reply(replyTo)(Rejected(s"Item '$itemId' was already added to this shopping cart"))
        else if (quantity <= 0)
          Effect.reply(replyTo)(Rejected("Quantity must be greater than zero"))
        else
          Effect
            .persist(ItemAdded(cartId, itemId, quantity))
            .thenReply(replyTo)(updatedCart => Accepted(updatedCart.toSummary))

      case RemoveItem(itemId, replyTo) =>
        if (state.hasItem(itemId))
          Effect.persist(ItemRemoved(cartId, itemId)).thenReply(replyTo)(updatedCart => Accepted(updatedCart.toSummary))
        else
          Effect.reply(replyTo)(Accepted(state.toSummary)) // removing an item is idempotent

      case AdjustItemQuantity(itemId, quantity, replyTo) =>
        if (quantity <= 0)
          Effect.reply(replyTo)(Rejected("Quantity must be greater than zero"))
        else if (state.hasItem(itemId))
          Effect
            .persist(ItemQuantityAdjusted(cartId, itemId, quantity))
            .thenReply(replyTo)(updatedCart => Accepted(updatedCart.toSummary))
        else
          Effect.reply(replyTo)(Rejected(s"Cannot adjust quantity for item '$itemId'. Item not present on cart"))

      case Checkout(replyTo) =>
        if (state.isEmpty)
          Effect.reply(replyTo)(Rejected("Cannot checkout an empty shopping cart"))
        else
          Effect
            .persist(CheckedOut(cartId, Instant.now()))
            .thenReply(replyTo)(updatedCart => Accepted(updatedCart.toSummary))

      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)
    }

  private def checkedOutShoppingCart(cartId: String, state: State, command: Command): ReplyEffect[Event, State] =
    command match {
      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)
      case cmd: AddItem =>
        Effect.reply(cmd.replyTo)(
          Rejected(s"Can't add an item to an already checked out shopping cart with id [$cartId]"))
      case cmd: RemoveItem =>
        Effect.reply(cmd.replyTo)(
          Rejected(s"Can't remove an item from an already checked out shopping cart with id [$cartId]"))
      case cmd: AdjustItemQuantity =>
        Effect.reply(cmd.replyTo)(
          Rejected(s"Can't adjust item on an already checked out shopping cart with id [$cartId]"))
      case cmd: Checkout =>
        Effect.reply(cmd.replyTo)(Rejected(s"Can't checkout already checked out shopping cart with id [$cartId]"))
    }

  private def handleEvent(state: State, event: Event) = {
    event match {
      case ItemAdded(_, itemId, quantity)            => state.updateItem(itemId, quantity)
      case ItemRemoved(_, itemId)                    => state.removeItem(itemId)
      case ItemQuantityAdjusted(_, itemId, quantity) => state.updateItem(itemId, quantity)
      case CheckedOut(_, eventTime)                  => state.checkout(eventTime)
    }
  }
}
