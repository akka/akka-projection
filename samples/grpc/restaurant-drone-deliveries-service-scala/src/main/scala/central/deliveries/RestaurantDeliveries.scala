package central.deliveries

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import central.CborSerializable
import central.Coordinates

import java.time.Instant

/**
 * Keeps track of registered deliveries for per restaurant
 */
object RestaurantDeliveries {

  // #commands
  sealed trait Command extends CborSerializable

  final case class RegisterDelivery(
      deliveryId: String,
      destination: Coordinates,
      replyTo: ActorRef[StatusReply[Done]])
      extends Command

  final case class SetUpRestaurant(
      localControlLocationId: String,
      restaurantLocation: Coordinates,
      replyTo: ActorRef[StatusReply[Done]])
      extends Command

  final case class ListCurrentDeliveries(replyTo: ActorRef[Seq[Delivery]])
      extends Command
  // #commands

  // #events
  sealed trait Event extends CborSerializable

  final case class DeliveryRegistered(delivery: Delivery) extends Event
  final case class RestaurantLocationSet(
      localControlLocationId: String,
      coordinates: Coordinates)
      extends Event

  final case class Delivery(
     deliveryId: String,
     // FIXME next two fields always the same for the same restaurant, annoying,
     //       but how else would we see them in downstream projection?
     localControlLocationId: String,
     origin: Coordinates,
     destination: Coordinates,
     timestamp: Instant)
  // #events

  // #state
  private final case class State(
      localControlLocationId: String,
      restaurantLocation: Coordinates,
      currentDeliveries: Vector[Delivery])
  // #state

  val EntityKey = EntityTypeKey[Command]("RestaurantDeliveries")

  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey)(entityContext =>
      RestaurantDeliveries(entityContext.entityId)))
  }

  def apply(restaurantId: String): Behavior[Command] =
    EventSourcedBehavior[Command, Event, Option[State]](
      PersistenceId(EntityKey.name, restaurantId),
      None,
      onCommand,
      onEvent).withTaggerForState {
      case (Some(state), _) =>
        // tag events with location id as topic, grpc projection filters makes sure only that location
        // picks them up for drone delivery
        Set("t:" + state.localControlLocationId)
      case _ => Set.empty
    }

  // #commandHandler
  private def onCommand(
      state: Option[State],
      command: Command): Effect[Event, Option[State]] =
    state match {
      case None        => onCommandNoState(command)
      case Some(state) => onCommandInitialized(state, command)
    }

  private def onCommandNoState(command: Command): Effect[Event, Option[State]] =
    command match {
      case RegisterDelivery(_, _, replyTo) =>
        Effect.reply(replyTo)(
          StatusReply.Error(
            "Restaurant not yet initialized, cannot accept registrations"))
      case ListCurrentDeliveries(replyTo) =>
        Effect.reply(replyTo)(Vector.empty)
      case SetUpRestaurant(locationId, coordinates, replyTo) =>
        Effect
          .persist(RestaurantLocationSet(locationId, coordinates))
          .thenReply(replyTo)(_ => StatusReply.Ack)
    }

  private def onCommandInitialized(
      state: State,
      command: Command): Effect[Event, Option[State]] = {
    command match {
      case RegisterDelivery(deliveryId, destination, replyTo) =>
        state.currentDeliveries.find(_.deliveryId == deliveryId) match {
          case Some(existing) if existing.destination == destination =>
            // already registered
            Effect.reply(replyTo)(StatusReply.Ack)
          case Some(_) =>
            Effect.reply(replyTo)(
              StatusReply.Error("Delivery id exists but for other destination"))
          case None =>
            Effect
              .persist(
                DeliveryRegistered(
                  Delivery(
                    deliveryId,
                    state.localControlLocationId,
                    state.restaurantLocation,
                    destination,
                    Instant.now())))
              .thenReply(replyTo)(_ => StatusReply.Ack)
        }
      case ListCurrentDeliveries(replyTo) =>
        Effect.reply(replyTo)(state.currentDeliveries)
      case setup: SetUpRestaurant =>
        Effect.reply(setup.replyTo)(
          StatusReply.Error("Changing restaurant location not supported"))
    }
  }
  // #commandHandler

  private def onEvent(state: Option[State], event: Event): Option[State] =
    (state, event) match {
      case (Some(state), DeliveryRegistered(delivery)) =>
        Some(
          state.copy(currentDeliveries = state.currentDeliveries :+ delivery))
      case (None, RestaurantLocationSet(localControlLocationId, location)) =>
        // initial setup of location
        Some(
          State(
            restaurantLocation = location,
            localControlLocationId = localControlLocationId,
            currentDeliveries = Vector.empty))
      case _ =>
        throw new RuntimeException("Unexpected event/state combination")
    }

}
