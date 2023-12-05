package shopping.cart

import java.time.Instant

import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.pattern.StatusReply
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicationContext
import akka.persistence.typed.scaladsl.ReplyEffect
import akka.persistence.typed.scaladsl.RetentionCriteria
import akka.projection.grpc.replication.scaladsl.ReplicatedBehaviors
import akka.projection.grpc.replication.scaladsl.Replication
import akka.projection.grpc.replication.scaladsl.Replication.EdgeReplication
import akka.projection.grpc.replication.scaladsl.ReplicationSettings
import akka.projection.r2dbc.scaladsl.R2dbcReplication
import akka.serialization.jackson.CborSerializable

/**
 * This is an event sourced actor (`EventSourcedBehavior`). An entity managed by Cluster Sharding.
 *
 * It has a state, [[ShoppingCart.State]], which holds the current shopping cart items and whether it's checked out.
 *
 * You interact with event sourced actors by sending commands to them, see classes implementing
 * [[ShoppingCart.Command]].
 *
 * The command handler validates and translates commands to events, see classes implementing [[ShoppingCart.Event]].
 * It's the events that are persisted by the `EventSourcedBehavior`. The event handler updates the current state based
 * on the event. This is done when the event is first created, and when the entity is loaded from the database - each
 * event will be replayed to recreate the state of the entity.
 *
 * This shopping cart is replicated using Replicated Event Sourcing. Multiple entity instances can be active at the same
 * time, so the state must be convergent, and each cart item is modelled as a counter. When checking out the cart, only
 * one of the replicas performs the actual checkout, once it's seen that all replicas have closed this cart which will
 * be after all item updated events have been replicated.
 */
object ShoppingCart {

  /**
   * The current state held by the `EventSourcedBehavior`.
   */
  //#checkoutStep1Event
  //#stateUpdateItem
  //#stateVipCustomer
  final case class State(
      items: Map[String, Int],
      closed: Set[ReplicaId],
      checkedOut: Option[Instant],
      vipCustomer: Boolean)
      extends CborSerializable {
    //#stateUpdateItem
    //#checkoutStep1Event
    //#stateVipCustomer

    def isClosed: Boolean =
      closed.nonEmpty

    //#stateUpdateItem
    def updateItem(itemId: String, quantity: Int): State =
      copy(items = items + (itemId -> (items.getOrElse(itemId, 0) + quantity)))
    //#stateUpdateItem

    def markCustomerVip(): State =
      if (vipCustomer) this
      else copy(vipCustomer = true)

    //#checkoutStep1Event
    def close(replica: ReplicaId): State =
      copy(closed = closed + replica)
    //#checkoutStep1Event

    def checkout(now: Instant): State =
      copy(checkedOut = Some(now))

    def toSummary: Summary = {
      val cartItems = items.collect {
        case (id, quantity) if quantity > 0 => id -> quantity
      }
      Summary(cartItems, isClosed)
    }

    def totalQuantity: Int =
      items.valuesIterator.sum

    //#stateVipCustomer

    def tags: Set[String] = {
      val total = totalQuantity
      val quantityTags =
        if (total == 0) Set.empty
        else if (total >= 100) Set(LargeQuantityTag)
        else if (total >= 10) Set(MediumQuantityTag)
        else Set(SmallQuantityTag)

      quantityTags ++ (if (vipCustomer) Set(VipCustomerTag) else Set.empty)
    }
    //#stateVipCustomer
  }

  object State {
    val empty: State = State(items = Map.empty, closed = Set.empty, checkedOut = None, vipCustomer = false)
  }

  /**
   * This interface defines all the commands (messages) that the ShoppingCart actor supports.
   */
  sealed trait Command extends CborSerializable

  /**
   * A command to add an item to the cart.
   *
   * It replies with `StatusReply[Summary]`, which is sent back to the caller when all the events emitted by this
   * command are successfully persisted.
   */
  final case class AddItem(itemId: String, quantity: Int, replyTo: ActorRef[StatusReply[Summary]]) extends Command

  /**
   * A command to remove an item from the cart.
   */
  final case class RemoveItem(itemId: String, quantity: Int, replyTo: ActorRef[StatusReply[Summary]]) extends Command

  final case class MarkCustomerVip(replyTo: ActorRef[StatusReply[Summary]]) extends Command

  /**
   * A command to check out the shopping cart.
   */
  final case class Checkout(replyTo: ActorRef[StatusReply[Summary]]) extends Command

  /**
   * Internal command to close a shopping cart that's being checked out.
   */
  case object CloseForCheckout extends Command

  /**
   * Internal command to complete the checkout for a shopping cart.
   */
  case object CompleteCheckout extends Command

  /**
   * A command to get the current state of the shopping cart.
   */
  final case class Get(replyTo: ActorRef[Summary]) extends Command

  /**
   * Summary of the shopping cart state, used in reply messages.
   */
  final case class Summary(items: Map[String, Int], checkedOut: Boolean) extends CborSerializable

  /**
   * This interface defines all the events that the ShoppingCart supports.
   */
  sealed trait Event extends CborSerializable

  // #itemUpdatedEvent
  final case class ItemUpdated(itemId: String, quantity: Int) extends Event
  // #itemUpdatedEvent

  final case class CustomerMarkedVip(timestamp: Instant) extends Event

  final case class Closed(replica: ReplicaId) extends Event

  final case class CheckedOut(eventTime: Instant) extends Event

  val SmallQuantityTag = "small"
  val MediumQuantityTag = "medium"
  val LargeQuantityTag = "large"
  val VipCustomerTag = "vip"

  val EntityType = "replicated-shopping-cart"

  // #init
  def init(implicit system: ActorSystem[_]): Replication[Command] = {
    val replicationSettings = ReplicationSettings[Command](EntityType, R2dbcReplication())
    Replication.grpcReplication(replicationSettings)(ShoppingCart.apply)
  }

  def apply(replicatedBehaviors: ReplicatedBehaviors[Command, Event, State]): Behavior[Command] = {
    Behaviors.setup[Command] { context =>
      replicatedBehaviors.setup { replicationContext =>
        new ShoppingCart(context, replicationContext).behavior()
      }
    }
  }
  // #init

  // Use `initWithProducerFilter` instead of `init` to enable filters based on tags.
  // The filter is defined to only replicate carts from VIP customers, other customer carts will stay in
  // the replica they were created (but can be marked VIP at any point in time before being closed)
  // #init-producerFilter
  def initWithProducerFilter(implicit system: ActorSystem[_]): Replication[Command] = {
    val producerFilter: EventEnvelope[Event] => Boolean = { envelope =>
      envelope.tags.contains(VipCustomerTag)
    }
    val replicationSettings = ReplicationSettings[Command](EntityType, R2dbcReplication())
      .withProducerFilter(producerFilter)

    Replication.grpcReplication(replicationSettings)(ShoppingCart.applyWithProducerFilter)
  }

  def applyWithProducerFilter(replicatedBehaviors: ReplicatedBehaviors[Command, Event, State]): Behavior[Command] = {
    Behaviors.setup[Command] { context =>
      replicatedBehaviors.setup { replicationContext =>
        new ShoppingCart(context, replicationContext, onlyReplicateVip = true).behavior()
      }
    }
  }
  // #init-producerFilter

  // #init-allow-edge
  def initAllowEdge(implicit system: ActorSystem[_]): EdgeReplication[Command] = {
    val replicationSettings = ReplicationSettings[Command](EntityType, R2dbcReplication())
      .withEdgeReplication(true)
    Replication.grpcEdgeReplication(replicationSettings)(ShoppingCart.apply)
  }
  // #init-allow-edge

  // #init-edge
  def initEdge(implicit system: ActorSystem[_]): EdgeReplication[Command] = {
    val replicationSettings = ReplicationSettings[Command](EntityType, R2dbcReplication())
    Replication.grpcEdgeReplication(replicationSettings)(ShoppingCart.apply)
  }
  // #init-edge

}

class ShoppingCart(
    context: ActorContext[ShoppingCart.Command],
    replicationContext: ReplicationContext,
    onlyReplicateVip: Boolean = false) {
  import ShoppingCart._

  // one of the replicas is responsible for checking out the shopping cart, once all replicas have closed
  // #leader
  private val isLeader: Boolean = {
    val orderedReplicas = replicationContext.allReplicas.toSeq.sortBy(_.id)
    val leaderIndex = math.abs(replicationContext.entityId.hashCode % orderedReplicas.size)
    orderedReplicas(leaderIndex) == replicationContext.replicaId
  }
  // #leader

  def behavior(): EventSourcedBehavior[Command, Event, State] = {
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        persistenceId = replicationContext.persistenceId,
        emptyState = State.empty,
        commandHandler = handleCommand,
        eventHandler = handleEvent)
      .withTaggerForState { case (state, _) =>
        state.tags
      }
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100))
      .onPersistFailure(SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
  }

  private def handleCommand(state: State, command: Command): ReplyEffect[Event, State] = {
    // The shopping cart behavior changes if it's closed / checked out or not.
    // The commands are handled differently for each case.
    if (state.isClosed)
      closedShoppingCart(state, command)
    else
      openShoppingCart(state, command)
  }

  private def openShoppingCart(state: State, command: Command): ReplyEffect[Event, State] = {
    command match {
      case AddItem(itemId, quantity, replyTo) =>
        Effect
          .persist(ItemUpdated(itemId, quantity))
          .thenReply(replyTo) { updatedCart =>
            StatusReply.Success(updatedCart.toSummary)
          }

      case RemoveItem(itemId, quantity, replyTo) =>
        Effect
          .persist(ItemUpdated(itemId, -quantity))
          .thenReply(replyTo) { updatedCart =>
            StatusReply.Success(updatedCart.toSummary)
          }

      // #checkoutStep1
      case Checkout(replyTo) =>
        Effect
          .persist(Closed(replicationContext.replicaId))
          .thenReply(replyTo) { updatedCart =>
            StatusReply.Success(updatedCart.toSummary)
          }
      // #checkoutStep1

      case MarkCustomerVip(replyTo) =>
        if (!state.vipCustomer)
          Effect
            .persist(CustomerMarkedVip(Instant.now()))
            .thenReply(replyTo)(updatedCart => StatusReply.Success(updatedCart.toSummary))
        else
          // already marked vip
          Effect.none.thenReply(replyTo)(updatedCart => StatusReply.Success(updatedCart.toSummary))

      case CloseForCheckout =>
        Effect
          .persist(Closed(replicationContext.replicaId))
          .thenNoReply()

      case CompleteCheckout =>
        // only closed shopping carts should be completable
        Effect.noReply

      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)
    }
  }

  private def closedShoppingCart(state: State, command: Command): ReplyEffect[Event, State] = {
    command match {
      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)
      case cmd: AddItem =>
        Effect.reply(cmd.replyTo)(StatusReply.Error("Can't add an item to an already checked out shopping cart"))
      case cmd: RemoveItem =>
        Effect.reply(cmd.replyTo)(StatusReply.Error("Can't remove an item from an already checked out shopping cart"))
      case cmd: MarkCustomerVip =>
        Effect.reply(cmd.replyTo)(StatusReply.Error("Can't mark customer vip on an already checked out shopping cart"))
      case cmd: Checkout =>
        Effect.reply(cmd.replyTo)(StatusReply.Error("Can't checkout already checked out shopping cart"))
      case CloseForCheckout =>
        Effect
          .persist(Closed(replicationContext.replicaId))
          .thenNoReply()
      case CompleteCheckout =>
        // TODO: trigger other effects from shopping cart checkout
        Effect
          .persist(CheckedOut(Instant.now()))
          .thenNoReply()
    }
  }

  //#checkoutEventTrigger
  private def handleEvent(state: State, event: Event): State = {
    val newState = event match {
      case ItemUpdated(itemId, quantity) =>
        state.updateItem(itemId, quantity)
      case CustomerMarkedVip(_) =>
        state.markCustomerVip()
      case Closed(replica) =>
        state.close(replica)
      case CheckedOut(eventTime) =>
        state.checkout(eventTime)
    }
    eventTriggers(newState, event)
    newState
  }

  private def eventTriggers(state: State, event: Event): Unit = {
    if (!replicationContext.recoveryRunning) {
      if (onlyReplicateVip && !state.vipCustomer) {
        // not replicated, no need to coordinate, we can close it right away
        context.self ! CompleteCheckout
      } else {
        event match {
          case _: Closed =>
            if (!state.closed(replicationContext.replicaId)) {
              context.self ! CloseForCheckout
            } else if (isLeader) {
              val allClosed = replicationContext.allReplicas.diff(state.closed).isEmpty
              if (allClosed) context.self ! CompleteCheckout
            }
          case _ =>
        }
      }
    }
  }
  //#checkoutEventTrigger
}
