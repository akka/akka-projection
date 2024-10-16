/*
 * Copyright (C) 2023-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.state

import scala.annotation.nowarn
import scala.concurrent.duration._

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId

// #changeEventHandler
import akka.persistence.typed.state.scaladsl.ChangeEventHandler
import akka.persistence.typed.state.scaladsl.DurableStateBehavior
import akka.persistence.typed.state.scaladsl.Effect

// #changeEventHandler
import akka.serialization.jackson.CborSerializable

object DurableStateChangeEventDocExample {
  // #changeEventHandler
  object ShoppingCart {

    // #changeEventHandler

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

      def removeItem(itemId: String): State =
        copy(items = items - itemId)
    }
    object State {
      val empty = State(items = Map.empty)
    }

    sealed trait Command extends CborSerializable

    final case class AddItem(itemId: String, quantity: Int, replyTo: ActorRef[Done]) extends Command
    final case class RemoveItem(itemId: String, replyTo: ActorRef[Done]) extends Command
    final case class DiscardCart(replyTo: ActorRef[Done]) extends Command

    sealed trait ChangeEvent extends CborSerializable
    final case class ItemAdded(itemId: String, quantity: Int) extends ChangeEvent
    final case class ItemRemoved(itemId: String) extends ChangeEvent
    case object CartRemoved extends ChangeEvent

    val EntityKey: EntityTypeKey[Command] = EntityTypeKey[Command]("ShoppingCart")

    @nowarn("msg=never used")
    // #changeEventHandler
    val changeEventHandler = ChangeEventHandler[Command, State, ChangeEvent](
      updateHandler = {
        case (previousState, newState, AddItem(itemId, quantity, _)) =>
          ItemAdded(itemId, quantity)
        case (previousState, newState, RemoveItem(itemId, _)) =>
          ItemRemoved(itemId)
        case (_, _, command @ DiscardCart(_)) =>
          throw new IllegalStateException(s"Unexpected command ${command.getClass}")
      },
      deleteHandler = { (previousState, command) =>
        CartRemoved
      })

    def apply(cartId: String): Behavior[Command] = {
      DurableStateBehavior
        .withEnforcedReplies[Command, State](
          PersistenceId(EntityKey.name, cartId),
          State.empty,
          (state, command) =>
            command match {
              case AddItem(itemId, quantity, replyTo) =>
                val newState = state.updateItem(itemId, quantity)
                Effect.persist(newState).thenReply(replyTo)(_ => Done)
              case RemoveItem(itemId, replyTo) =>
                val newState = state.removeItem(itemId)
                Effect.persist(newState).thenReply(replyTo)(_ => Done)
              case DiscardCart(replyTo) =>
                Effect.delete().thenReply(replyTo)(_ => Done)
            })
        .withChangeEventHandler(changeEventHandler)
        .onPersistFailure(SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
    }

  }
  // #changeEventHandler

}
