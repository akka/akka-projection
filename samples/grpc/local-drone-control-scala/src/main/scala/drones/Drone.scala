/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package drones

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplyEffect

object Drone {
  sealed trait Command

  final case class ReportPosition(position: Position, replyTo: ActorRef[Done])
      extends Command

  sealed trait Event extends CborSerializable
  final case class PositionUpdated(position: Position) extends Event
  final case class CoarseGrainedLocationChanged(
      coordinates: CoarseGrainedCoordinates)
      extends Event

  final case class State(
      currentPosition: Option[Position],
      historicalPositions: Vector[Position]) extends CborSerializable {
    def coarseGrainedCoordinates: Option[CoarseGrainedCoordinates] =
      currentPosition.map(p =>
        CoarseGrainedCoordinates.fromCoordinates(p.coordinates))
  }

  private val emptyState = State(None, Vector.empty)

  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("Drone")

  val LocationHistoryLimit = 100

  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey)(entityContext =>
      Drone(entityContext.entityId)))
  }

  def apply(entityId: String): Behavior[Command] =
    EventSourcedBehavior[Command, Event, State](
      PersistenceId(EntityKey.name, entityId),
      emptyState,
      handleCommand,
      handleEvent)

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
          // new grid location
          Effect
            .persist(
              PositionUpdated(position),
              CoarseGrainedLocationChanged(newCoarseGrainedLocation))
            .thenReply(replyTo)(_ => Done)
        }
      }
  }

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

}
