package local.drones

import scala.concurrent.duration._
import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplyEffect
import akka.serialization.jackson.CborSerializable

object Drone {

  // #commands

  /**
   * This interface defines all the commands (messages) that the Drone actor supports.
   */
  sealed trait Command extends CborSerializable

  /**
   * A command to report the current position (coordinates and altitude) of the drone.
   *
   * It replies with `Done`, which is sent back to the caller when
   * all the events emitted by this command are successfully persisted.
   */
  final case class ReportPosition(position: Position, replyTo: ActorRef[Done])
      extends Command

  /**
   * A command to query the current position (coordinates and altitude) of the drone.
   *
   * It replies with a `StatusReply[Position]`, which is sent back to the caller as a success if the
   * coordinates are known. If not an error is sent back.
   */
  final case class GetCurrentPosition(replyTo: ActorRef[StatusReply[Position]])
      extends Command

  // #commands

  // #events

  /**
   * This interface defines all the events that the Drone supports.
   */
  sealed trait Event extends CborSerializable
  final case class PositionUpdated(position: Position) extends Event
  final case class CoarseGrainedLocationChanged(
      coordinates: CoarseGrainedCoordinates)
      extends Event
  // #events

  // #state
  final case class State(
      currentPosition: Option[Position],
      historicalPositions: Vector[Position])
      extends CborSerializable {
    def coarseGrainedCoordinates: Option[CoarseGrainedCoordinates] =
      currentPosition.map(p =>
        CoarseGrainedCoordinates.fromCoordinates(p.coordinates))
  }
  // #state

  private val emptyState = State(None, Vector.empty)

  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("Drone")

  val LocationHistoryLimit = 100

  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey)(entityContext =>
      Drone(entityContext.entityId)))
  }

  def apply(entityId: String): Behavior[Command] =
    Behaviors.setup { context =>
      val telemetry = Telemetry(context.system)
      telemetry.droneEntityActivated()
      EventSourcedBehavior[Command, Event, State](
        PersistenceId(EntityKey.name, entityId),
        emptyState,
        handleCommand,
        handleEvent)
        .onPersistFailure(
          SupervisorStrategy.restartWithBackoff(100.millis, 5.seconds, 0.1))
        .receiveSignal { case (_, PostStop) =>
          telemetry.droneEntityPassivated()
        }
        // #startFromSnapshot
        .snapshotWhen { (_, event, _) =>
          event.isInstanceOf[CoarseGrainedLocationChanged]
        }
    // #startFromSnapshot
    }

  // #commandHandler
  private def handleCommand(
      state: State,
      command: Command): ReplyEffect[Event, State] = command match {
    case ReportPosition(position, replyTo) =>
      if (state.currentPosition.contains(position))
        // already seen
        Effect.reply(replyTo)(Done)
      else {
        val newCoarseGrainedLocation =
          CoarseGrainedCoordinates.fromCoordinates(position.coordinates)
        if (state.coarseGrainedCoordinates.contains(newCoarseGrainedLocation)) {
          // same grid location as before
          Effect
            .persist(PositionUpdated(position))
            .thenReply(replyTo)(_ => Done)
        } else {
          // no previous location known or new grid location
          Effect
            .persist(
              PositionUpdated(position),
              CoarseGrainedLocationChanged(newCoarseGrainedLocation))
            .thenReply(replyTo)(_ => Done)
        }
      }

    case GetCurrentPosition(replyTo) =>
      state.currentPosition match {
        case Some(position) =>
          Effect.reply(replyTo)(StatusReply.Success(position))
        case None =>
          Effect.reply(replyTo)(
            StatusReply.Error("Position of drone is unknown"))
      }

  }

  // #commandHandler

  // #eventHandler
  private def handleEvent(state: State, event: Event): State = event match {
    case PositionUpdated(newPosition) =>
      val newHistoricalPositions = state.currentPosition match {
        case Some(position) =>
          val withPreviousPosition = state.historicalPositions :+ position
          if (withPreviousPosition.size > LocationHistoryLimit)
            withPreviousPosition.tail
          else withPreviousPosition
        case None => state.historicalPositions
      }
      state.copy(
        currentPosition = Some(newPosition),
        historicalPositions = newHistoricalPositions)
    case _: CoarseGrainedLocationChanged =>
      // can be derived from position, so not really updating state,
      // persisted as events for aggregation
      state

  }
  // #eventHandler

}
