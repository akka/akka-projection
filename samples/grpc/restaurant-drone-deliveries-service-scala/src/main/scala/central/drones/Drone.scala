package central.drones

import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.{DurableStateBehavior, Effect}

object Drone {

  sealed trait Command

  final case class UpdateLocation()

  val EntityKey = EntityTypeKey[Command]("central-drone")

  private final case class State()

  private val emptyState = State()

  def apply(droneId: String): Behavior[Command] =
    DurableStateBehavior(
      PersistenceId(EntityKey.name, droneId),
      emptyState,
      onCommand
    )

  private def onCommand(state: State, command: Command): Effect[State] = command match {
    case _ => Effect.none
  }

}
