package shopping.cart

import java.time.Instant

import scala.concurrent.duration._

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

/**
 * This is an event sourced actor (`EventSourcedBehavior`). An entity managed by Cluster Sharding.
 *
 * It has a state, [[ShoppingCart.State]], which holds the current shopping cart items
 * and whether it's checked out.
 *
 * You interact with event sourced actors by sending commands to them,
 * see classes implementing [[ShoppingCart.Command]].
 *
 * The command handler validates and translates commands to events, see classes implementing [[ShoppingCart.Event]].
 * It's the events that are persisted by the `EventSourcedBehavior`. The event handler updates the current
 * state based on the event. This is done when the event is first created, and when the entity is
 * loaded from the database - each event will be replayed to recreate the state
 * of the entity.
 */
//#tags
object ShoppingCart {
  //#tags

  /**
   * The current state held by the `EventSourcedBehavior`.
   */
  //#tags
  final case class State(
      items: Map[String, Int],
      checkoutDate: Option[Instant],
      customerId: String,
      customerCategory: String)
      extends CborSerializable {

    //#tags

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

    def setCustomer(customerId: String, category: String): State =
      copy(customerId = customerId, customerCategory = category)

    def toSummary: Summary =
      Summary(items, isCheckedOut, customerId, customerCategory)

    //#tags
    def totalQuantity: Int =
      items.valuesIterator.sum

    def tags: Set[String] = {
      val total = totalQuantity
      if (total == 0) Set.empty
      else if (total >= 100) Set(LargeQuantityTag)
      else if (total >= 10) Set(MediumQuantityTag)
      else Set(SmallQuantityTag)
    }

  }

  //#tags

  object State {
    val empty: State =
      State(
        items = Map.empty,
        checkoutDate = None,
        customerId = "",
        customerCategory = "")
  }

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
   * A command to remove an item from the cart.
   */
  final case class RemoveItem(
      itemId: String,
      replyTo: ActorRef[StatusReply[Summary]])
      extends Command

  /**
   * A command to adjust the quantity of an item in the cart.
   */
  final case class AdjustItemQuantity(
      itemId: String,
      quantity: Int,
      replyTo: ActorRef[StatusReply[Summary]])
      extends Command

  /**
   * A command to checkout the shopping cart.
   */
  final case class Checkout(replyTo: ActorRef[StatusReply[Summary]])
      extends Command

  /**
   * A command to get the current state of the shopping cart.
   */
  final case class Get(replyTo: ActorRef[Summary]) extends Command

  /**
   * Summary of the shopping cart state, used in reply messages.
   */
  final case class Summary(
      items: Map[String, Int],
      checkedOut: Boolean,
      customerId: String,
      customerCategory: String)
      extends CborSerializable

  final case class SetCustomer(
      customerId: String,
      category: String,
      replyTo: ActorRef[StatusReply[Summary]])
      extends Command

  /**
   * This interface defines all the events that the ShoppingCart supports.
   */
  sealed trait Event extends CborSerializable {
    def cartId: String
  }

  final case class ItemAdded(cartId: String, itemId: String, quantity: Int)
      extends Event

  final case class ItemRemoved(cartId: String, itemId: String, oldQuantity: Int)
      extends Event

  final case class ItemQuantityAdjusted(
      cartId: String,
      itemId: String,
      newQuantity: Int,
      oldQuantity: Int)
      extends Event

  final case class CheckedOut(cartId: String, eventTime: Instant) extends Event

  final case class CustomerDefined(
      cartId: String,
      customerId: String,
      category: String)
      extends Event

  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("ShoppingCart")

  //#tags
  val SmallQuantityTag = "small"
  val MediumQuantityTag = "medium"
  val LargeQuantityTag = "large"

  //#tags

  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey)(entityContext =>
      ShoppingCart(entityContext.entityId)))
  }

  //#tags
  def apply(cartId: String): Behavior[Command] = {
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        persistenceId = PersistenceId(EntityKey.name, cartId),
        emptyState = State.empty,
        commandHandler =
          (state, command) => handleCommand(cartId, state, command),
        eventHandler = (state, event) => handleEvent(state, event))
      .withTaggerForState { case (state, _) =>
        state.tags
      }
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100))
      .onPersistFailure(
        SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
  }
  //#tags

  private def handleCommand(
      cartId: String,
      state: State,
      command: Command): ReplyEffect[Event, State] = {
    // The shopping cart behavior changes if it's checked out or not.
    // The commands are handled differently for each case.
    if (state.isCheckedOut)
      checkedOutShoppingCart(cartId, state, command)
    else
      openShoppingCart(cartId, state, command)
  }

  private def openShoppingCart(
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
              StatusReply.Success(updatedCart.toSummary)
            }

      case RemoveItem(itemId, replyTo) =>
        if (state.hasItem(itemId))
          Effect
            .persist(ItemRemoved(cartId, itemId, state.items(itemId)))
            .thenReply(replyTo)(updatedCart =>
              StatusReply.Success(updatedCart.toSummary))
        else
          Effect.reply(replyTo)(
            StatusReply.Success(state.toSummary)
          ) // removing an item is idempotent

      case AdjustItemQuantity(itemId, quantity, replyTo) =>
        if (quantity <= 0)
          Effect.reply(replyTo)(
            StatusReply.Error("Quantity must be greater than zero"))
        else if (state.hasItem(itemId))
          Effect
            .persist(
              ItemQuantityAdjusted(
                cartId,
                itemId,
                quantity,
                state.items(itemId)))
            .thenReply(replyTo)(updatedCart =>
              StatusReply.Success(updatedCart.toSummary))
        else
          Effect.reply(replyTo)(StatusReply.Error(
            s"Cannot adjust quantity for item '$itemId'. Item not present on cart"))

      case Checkout(replyTo) =>
        if (state.isEmpty)
          Effect.reply(replyTo)(
            StatusReply.Error("Cannot checkout an empty shopping cart"))
        else
          Effect
            .persist(CheckedOut(cartId, Instant.now()))
            .thenReply(replyTo)(updatedCart =>
              StatusReply.Success(updatedCart.toSummary))

      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)

      case SetCustomer(customerId, category, replyTo) =>
        Effect
          .persist(CustomerDefined(cartId, customerId, category))
          .thenReply(replyTo)(updatedCart =>
            StatusReply.Success(updatedCart.toSummary))
    }
  }

  private def checkedOutShoppingCart(
      cartId: String,
      state: State,
      command: Command): ReplyEffect[Event, State] = {
    command match {
      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)
      case SetCustomer(customerId, category, replyTo) =>
        Effect
          .persist(CustomerDefined(cartId, customerId, category))
          .thenReply(replyTo)(updatedCart =>
            StatusReply.Success(updatedCart.toSummary))
      case cmd: AddItem =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't add an item to an already checked out shopping cart"))
      case cmd: RemoveItem =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't remove an item from an already checked out shopping cart"))
      case cmd: AdjustItemQuantity =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't adjust item on an already checked out shopping cart"))
      case cmd: Checkout =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error("Can't checkout already checked out shopping cart"))
    }
  }

  private def handleEvent(state: State, event: Event): State = {
    event match {
      case ItemAdded(_, itemId, quantity) =>
        state.updateItem(itemId, quantity)
      case ItemRemoved(_, itemId, _) =>
        state.removeItem(itemId)
      case ItemQuantityAdjusted(_, itemId, quantity, _) =>
        state.updateItem(itemId, quantity)
      case CheckedOut(_, eventTime) =>
        state.checkout(eventTime)
      case CustomerDefined(_, customerId, category) =>
        state.setCustomer(customerId, category)
    }
  }
  //#tags
}
//#tags
