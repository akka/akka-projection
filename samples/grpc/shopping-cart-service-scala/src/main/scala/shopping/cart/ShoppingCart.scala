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
  //#state
  //#tags
  final case class State(
      items: Map[String, Int],
      checkoutDate: Option[Instant])
      extends CborSerializable {

    //#tags

    def isCheckedOut: Boolean =
      checkoutDate.isDefined

    def isEmpty: Boolean =
      items.isEmpty

    def updateItem(itemId: String, quantity: Int): State = {
      val newQuantity = items.getOrElse(itemId, 0) + quantity
      if (newQuantity > 0)
        copy(items = items + (itemId -> newQuantity))
      else
        copy(items = items.removed(itemId))
    }

    def checkout(now: Instant): State =
      copy(checkoutDate = Some(now))

    def toSummary: Summary = {
      // filter out removed items
      Summary(items, isCheckedOut)
    }

    //#tags
    def totalQuantity: Int =
      items.map { case (_, quantity) => quantity }.sum

    def tags: Set[String] = {
      val total = totalQuantity
      if (total == 0) Set.empty
      else if (total >= 100) Set(LargeQuantityTag)
      else if (total >= 10) Set(MediumQuantityTag)
      else Set(SmallQuantityTag)
    }

  }
  //#state

  //#tags

  object State {
    val empty: State =
      State(
        items = Map.empty,
        checkoutDate = None)
  }

  //#commands
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
      checkedOut: Boolean)
      extends CborSerializable
  //#commands

  //#events
  /**
   * This interface defines all the events that the ShoppingCart supports.
   */
  sealed trait Event extends CborSerializable

  final case class ItemUpdated(itemId: String, quantity: Int) extends Event

  final case class CheckedOut(eventTime: Instant) extends Event
  //#events

  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("ShoppingCart")

  //#tags
  val SmallQuantityTag = "small"
  val MediumQuantityTag = "medium"
  val LargeQuantityTag = "large"

  //#tags

  //#init
  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey)(entityContext =>
      ShoppingCart(entityContext.entityId)))
  }
  //#init

  //#init
  //#tags
  def apply(cartId: String): Behavior[Command] = {
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        persistenceId = PersistenceId(EntityKey.name, cartId),
        emptyState = State.empty,
        commandHandler =
          (state, command) => handleCommand(state, command),
        eventHandler = (state, event) => handleEvent(state, event))
      .withTaggerForState { case (state, _) =>
        state.tags
      }
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100))
      .onPersistFailure(
        SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
  }
  //#tags
  //#init
  private def handleCommand(
      state: State,
      command: Command): ReplyEffect[Event, State] = {
    // The shopping cart behavior changes if it's checked out or not.
    // The commands are handled differently for each case.
    if (state.isCheckedOut)
      checkedOutShoppingCart(state, command)
    else
      openShoppingCart(state, command)
  }

  //#commandHandler
  private def openShoppingCart(
      state: State,
      command: Command): ReplyEffect[Event, State] = {
    command match {
      case AddItem(itemId, quantity, replyTo) =>
       if (quantity <= 0)
          Effect.reply(replyTo)(
            StatusReply.Error("Quantity must be greater than zero"))
        else
          Effect
            .persist(ItemUpdated(itemId, quantity))
            .thenReply(replyTo) { updatedCart =>
              StatusReply.Success(updatedCart.toSummary)
            }

      case RemoveItem(itemId, quantity, replyTo) =>
        if (quantity <= 0)
          Effect.reply(replyTo)(
            StatusReply.Error("Quantity must be greater than zero"))
        else
          Effect
            .persist(ItemUpdated(itemId, -quantity))
            .thenReply(replyTo)(updatedCart =>
              StatusReply.Success(updatedCart.toSummary))


      case Checkout(replyTo) =>
        if (state.isEmpty)
          Effect.reply(replyTo)(
            StatusReply.Error("Cannot checkout an empty shopping cart"))
        else
          Effect
            .persist(CheckedOut(Instant.now()))
            .thenReply(replyTo)(updatedCart =>
              StatusReply.Success(updatedCart.toSummary))

      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)
    }
  }
  //#commandHandler

  private def checkedOutShoppingCart(
      state: State,
      command: Command): ReplyEffect[Event, State] = {
    command match {
      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)
      case cmd: AddItem =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't add an item to an already checked out shopping cart"))
      case cmd: RemoveItem =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't remove an item from an already checked out shopping cart"))
      case cmd: Checkout =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error("Can't checkout already checked out shopping cart"))
    }
  }

  //#eventHandler
  private def handleEvent(state: State, event: Event): State = {
    event match {
      case ItemUpdated(itemId, quantity) =>
        state.updateItem(itemId, quantity)
      case CheckedOut(eventTime) =>
        state.checkout(eventTime)
    }
  }
  //#eventHandler

  //#tags
}
//#tags
