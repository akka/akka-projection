/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package charging

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.pattern.StatusReply
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicationContext
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.replication.scaladsl.ReplicatedBehaviors
import akka.projection.grpc.replication.scaladsl.Replication
import akka.projection.grpc.replication.scaladsl.Replication.EdgeReplication
import akka.projection.grpc.replication.scaladsl.ReplicationSettings
import akka.projection.r2dbc.scaladsl.R2dbcReplication
import akka.serialization.jackson.CborSerializable

import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.DurationLong
import scala.concurrent.duration.FiniteDuration
import scala.math.Ordered.orderingToOrdered

object ChargingStation {

  // commands and replies
  sealed trait Command extends CborSerializable {}
  case class Create(
      locationId: String,
      chargingSlots: Int,
      replyTo: ActorRef[StatusReply[Done]])
      extends Command
  case class StartCharging(
      droneId: String,
      replyTo: ActorRef[StatusReply[StartChargingResponse]])
      extends Command
  sealed trait StartChargingResponse extends CborSerializable
  case class AllSlotsBusy(firstSlotFreeAt: Instant)
      extends StartChargingResponse

  case class GetState(replyTo: ActorRef[StatusReply[ChargingStation.State]])
      extends Command

  private case class CompleteCharging(droneId: String) extends Command

  // events
  sealed trait Event extends CborSerializable {}
  case class Created(locationId: String, chargingSlots: Int) extends Event
  case class ChargingStarted(droneId: String, chargeComplete: Instant)
      extends Event
      with StartChargingResponse

  case class ChargingCompleted(droneId: String) extends Event

  case class ChargingDrone(
      droneId: String,
      chargingDone: Instant,
      replicaId: String)
  case class State(
      chargingSlots: Int,
      dronesCharging: Set[ChargingDrone],
      stationLocationId: String)
      extends CborSerializable

  val EntityType = "charging-station"

  private val FullChargeTime = 5.minutes

  /**
   * Init for running in edge node, this is the only difference from the ChargingStation
   * in restaurant-deliveries-service
   */
  def initEdge(locationId: String)(
      implicit system: ActorSystem[_]): EdgeReplication[Command] = {
    val replicationSettings =
      ReplicationSettings[Command](EntityType, R2dbcReplication())
        .withSelfReplicaId(ReplicaId(locationId))
        .withInitialConsumerFilter(
          // only replicate charging stations local to the edge system
          Seq(
            ConsumerFilter.excludeAll,
            ConsumerFilter.IncludeTopics(Set(locationId))))
    Replication.grpcEdgeReplication(replicationSettings)(ChargingStation.apply)
  }

  /**
   * Init for running in cloud replica
   */
  def init(implicit system: ActorSystem[_]): Replication[Command] = {
    val replicationSettings =
      ReplicationSettings[Command](EntityType, R2dbcReplication())
        // FIXME remove once release out with flag in config (1.5.1-M2/GA)
        .withEdgeReplication(true)
    Replication.grpcReplication(replicationSettings)(ChargingStation.apply)
  }

  def apply(
      replicatedBehaviors: ReplicatedBehaviors[Command, Event, Option[State]])
      : Behavior[Command] = {
    Behaviors.setup[Command] { context =>
      Behaviors.withTimers { timers =>
        replicatedBehaviors.setup { replicationContext =>
          context.log.info(
            "Charging Station {} starting up",
            replicationContext.entityId)
          new ChargingStation(context, replicationContext, timers).behavior()
        }
      }
    }
  }

  private def durationUntil(instant: Instant): FiniteDuration =
    (instant.getEpochSecond - Instant.now().getEpochSecond).seconds

}

class ChargingStation(
    context: ActorContext[ChargingStation.Command],
    replicationContext: ReplicationContext,
    timers: TimerScheduler[ChargingStation.Command]) {

  import ChargingStation._

  def behavior(): EventSourcedBehavior[Command, Event, Option[State]] =
    EventSourcedBehavior(
      replicationContext.persistenceId,
      None,
      handleCommand,
      handleEvent)
      .receiveSignal { case (Some(state: State), RecoveryCompleted) =>
        handleRecoveryCompleted(state)
      }
      // tag with location id so we can replicate only to the right edge node
      .withTaggerForState {
        case (None, _)        => Set.empty
        case (Some(state), _) => Set("t:" + state.stationLocationId)
      }

  private def handleCommand(
      state: Option[State],
      command: Command): Effect[Event, Option[State]] =
    state match {
      case None        => handleCommandNoState(command)
      case Some(state) => handleCommandInitialized(state, command)
    }

  private def handleCommandNoState(
      command: Command): Effect[Event, Option[State]] =
    command match {
      case Create(locationId, chargingSlots, replyTo) =>
        Effect
          .persist(Created(locationId, chargingSlots))
          .thenReply(replyTo)(_ => StatusReply.Ack)
      case StartCharging(_, replyTo) =>
        Effect.reply(replyTo)(
          StatusReply.Error(
            s"Charging station ${replicationContext.entityId} not initialized"))
      case GetState(replyTo) =>
        Effect.reply(replyTo)(
          StatusReply.Error(
            s"Charging station ${replicationContext.entityId} not initialized"))
      case unexpected =>
        context.log.warn(
          "Got an unexpected command {} but charging station with id {} not initialized",
          unexpected.getClass,
          replicationContext.entityId)
        Effect.none
    }

  private def handleCommandInitialized(
      state: ChargingStation.State,
      command: ChargingStation.Command): Effect[Event, Option[State]] = {
    command match {
      case Create(_, _, replyTo) =>
        Effect.reply(replyTo)(StatusReply.Error(
          s"Got a create command, but station id ${replicationContext.entityId} was already created"))

      case StartCharging(droneId, replyTo) =>
        if (state.dronesCharging.exists(_.droneId == droneId)) {
          context.log.warn(
            "Drone {} requested charging but is already charging. Ignoring.",
            droneId)
          Effect.none
        } else if (state.dronesCharging.size >= state.chargingSlots) {
          val earliestFreeSlot = state.dronesCharging.map(_.chargingDone).min
          context.log.info(
            "Drone {} requested charging but all stations busy, earliest free slot {}",
            droneId,
            earliestFreeSlot)
          Effect.reply(replyTo)(
            StatusReply.Success(AllSlotsBusy(earliestFreeSlot)))
        } else {
          // charge
          val chargeCompletedBy =
            Instant.now().plusSeconds(FullChargeTime.toSeconds)
          context.log.info(
            "Drone {} requested charging, will complete charging at {}",
            droneId,
            chargeCompletedBy)
          val event = ChargingStarted(droneId, chargeCompletedBy)
          Effect.persist(event).thenRun { (_: Option[State]) =>
            timers.startSingleTimer(
              CompleteCharging(droneId),
              durationUntil(chargeCompletedBy))
            // Note: The event is also the reply
            replyTo ! StatusReply.Success(event)
          }
        }

      case CompleteCharging(droneId) =>
        if (state.dronesCharging.exists(_.droneId == droneId)) {
          context.log.info("Drone {} completed charging", droneId)
          Effect.persist(ChargingCompleted(droneId))
        } else {
          Effect.none
        }

      case GetState(replyTo) =>
        Effect.reply(replyTo)(StatusReply.Success(state))

    }
  }

  private def handleEvent(state: Option[State], event: Event): Option[State] = {
    state match {
      case None =>
        event match {
          case Created(locationId, chargingSlots) =>
            Some(State(chargingSlots, Set.empty, locationId))
          case unexpected =>
            throw new IllegalArgumentException(
              s"Got unexpected event ${unexpected} for uninitialized state")
        }

      case Some(state) =>
        event match {
          case Created(_, _) =>
            context.log.warn("Saw a second created event, ignoring")
            Some(state)
          case ChargingStarted(droneId, chargeComplete) =>
            Some(
              state.copy(dronesCharging = state.dronesCharging + ChargingDrone(
                droneId,
                chargeComplete,
                replicationContext.origin.id)))
          case ChargingCompleted(droneId) =>
            Some(
              state.copy(dronesCharging =
                state.dronesCharging.filterNot(_.droneId == droneId)))
        }

    }
  }

  private def handleRecoveryCompleted(state: State): Unit = {
    // FIXME this scheme is not quite reliable, because station is not remembered/restarted
    //       until next new event/command if edge system is shut down/restarted
    // Complete or set up timers for completion for drones charging,
    // but only if the charging was initiated in this replica
    val now = Instant.now()
    state.dronesCharging
      .filter(_.replicaId == replicationContext.replicaId.id)
      .foreach { chargingDrone =>
        if (chargingDrone.chargingDone < now)
          context.self ! CompleteCharging(chargingDrone.droneId)
        else
          timers.startSingleTimer(
            CompleteCharging(chargingDrone.droneId),
            durationUntil(chargingDrone.chargingDone))
      }
  }

}
