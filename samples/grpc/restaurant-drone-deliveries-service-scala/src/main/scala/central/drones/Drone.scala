package central.drones

import akka.Done
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.cluster.sharding.typed.scaladsl.{
  ClusterSharding,
  Entity,
  EntityTypeKey
}
import akka.pattern.StatusReply
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.{ DurableStateBehavior, Effect }
import central.CborSerializable
import local.drones.CoarseGrainedCoordinates

import java.time.Instant

object Drone {

  sealed trait Command

  final case class UpdateLocation(
      locationName: String,
      coarseGrainedCoordinates: CoarseGrainedCoordinates,
      replyTo: ActorRef[StatusReply[Done]])
      extends Command

  val EntityKey = EntityTypeKey[Command]("CentralDrone")

  final case class State(
      locationName: String,
      currentLocation: Option[CoarseGrainedCoordinates],
      lastChange: Instant)
      extends CborSerializable

  private val emptyState = State("unknown", None, Instant.EPOCH)

  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey)(entityContext =>
      Drone(entityContext.entityId)))
  }

  def apply(droneId: String): Behavior[Command] = {
    Behaviors.setup { context =>
      DurableStateBehavior(
        PersistenceId(EntityKey.name, droneId),
        emptyState,
        onCommand(context))
    }
  }

  private def onCommand(context: ActorContext[Command])(
      state: State,
      command: Command): Effect[State] =
    command match {
      case UpdateLocation(locationName, coordinates, replyTo) =>
        context.log.info(
          "Updating location to [{}], [{}]",
          locationName,
          coordinates)
        Effect
          .persist(
            state.copy(
              locationName = locationName,
              currentLocation = Some(coordinates)))
          .thenReply(replyTo)(_ => StatusReply.ack())
    }

}

/**
 * Write local drone control location name column for querying drone locations per control location
 */
final class LocationColumn extends AdditionalColumn[Drone.State, String] {

  override def columnName: String = "location"

  override def bind(upsert: AdditionalColumn.Upsert[Drone.State])
      : AdditionalColumn.Binding[String] =
    AdditionalColumn.BindValue(upsert.value.locationName)

}
