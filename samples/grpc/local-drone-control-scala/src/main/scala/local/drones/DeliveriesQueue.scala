package local.drones

import akka.Done
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.{ ActorRef, Behavior }
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.{ DurableStateBehavior, Effect }

import java.time.Instant

object DeliveriesQueue {

  sealed trait Command extends CborSerializable

  final case class AddDelivery(
      waitingDelivery: WaitingDelivery,
      replyTo: ActorRef[Done])
      extends Command

  final case class RequestDelivery(
      droneId: String,
      droneCoordinates: Coordinates,
      replyTo: ActorRef[StatusReply[WaitingDelivery]])
      extends Command

  final case class CompleteDelivery(
      deliveryId: String,
      replyTo: ActorRef[StatusReply[Done]])
      extends Command

  final case class GetCurrentState(replyTo: ActorRef[State]) extends Command

  final case class WaitingDelivery(
      deliveryId: String,
      from: Coordinates,
      to: Coordinates)

  final case class DeliveryInProgress(
      deliveryId: String,
      droneId: String,
      pickupTime: Instant)
  final case class State(
      waitingDeliveries: Vector[WaitingDelivery],
      deliveriesInProgress: Vector[DeliveryInProgress])
      extends CborSerializable

  // Not really an entity, we just have one
  val EntityKey = EntityTypeKey("RestaurantDeliveries")

  def apply(): Behavior[Command] = {
    Behaviors.setup { context =>
      DurableStateBehavior[Command, State](
        PersistenceId(EntityKey.name, "DeliveriesQueue"),
        State(Vector.empty, Vector.empty),
        onCommand(context))
    }
  }

  private def onCommand(context: ActorContext[Command])(
      state: State,
      command: Command): Effect[State] =
    command match {
      case AddDelivery(delivery, replyTo) =>
        context.log.info("Adding delivery [{}] to queue", delivery.deliveryId)
        if (state.waitingDeliveries.contains(delivery))
          Effect.reply(replyTo)(Done)
        else
          Effect
            .persist(
              state.copy(waitingDeliveries =
                state.waitingDeliveries :+ delivery))
            .thenReply(replyTo)(_ => Done)

      case RequestDelivery(droneId, droneCoordinates, replyTo) =>
        if (state.waitingDeliveries.isEmpty)
          Effect.reply(replyTo)(StatusReply.Error("No waiting orders"))
        else {
          val closestPickupForDrone = state.waitingDeliveries.minBy(delivery =>
            droneCoordinates.distanceTo(delivery.from))
          context.log.info(
            "Selected next delivery [{}] for drone [{}]",
            closestPickupForDrone.deliveryId,
            droneId)
          // Note: A real application would have to care more about retries/lost data here
          Effect
            .persist(
              state.copy(
                waitingDeliveries =
                  state.waitingDeliveries.filterNot(_ == closestPickupForDrone),
                state.deliveriesInProgress :+ DeliveryInProgress(
                  closestPickupForDrone.deliveryId,
                  droneId,
                  Instant.now())))
            .thenReply(replyTo)(_ => StatusReply.Success(closestPickupForDrone))
        }

      case CompleteDelivery(deliveryId, replyTo) =>
        if (!state.deliveriesInProgress.exists(_.deliveryId == deliveryId)) {
          Effect.reply(replyTo)(
            StatusReply.Error(s"Unknown delivery id: ${deliveryId}"))
        } else {
          Effect
            .persist(
              state.copy(deliveriesInProgress =
                state.deliveriesInProgress.filter(_.deliveryId == deliveryId)))
            .thenReply(replyTo)(_ => StatusReply.Success(Done))
        }

      case GetCurrentState(replyTo) =>
        Effect.reply(replyTo)(state)
    }

}
