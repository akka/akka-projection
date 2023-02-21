/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection
import akka.actor.typed.scaladsl.LoggerOps
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.PreRestart
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.StashBuffer
import akka.annotation.InternalApi
import akka.projection.internal.ManagementState
import akka.projection.scaladsl.ProjectionManagement

object ProjectionBehavior {

  sealed trait Command

  /**
   * The `ProjectionBehavior` and its `Projection` can be stopped with this message.
   */
  object Stop extends Command

  /**
   * INTERNAL API
   */
  @InternalApi private[projection] object Internal {

    object Stopped extends Command

    sealed trait ProjectionManagementCommand extends Command
    final case class GetOffset[Offset](projectionId: ProjectionId, replyTo: ActorRef[CurrentOffset[Offset]])
        extends ProjectionManagementCommand
    final case class CurrentOffset[Offset](projectionId: ProjectionId, offset: Option[Offset])
    final case class GetOffsetResult[Offset](offset: Option[Offset], replyTo: ActorRef[CurrentOffset[Offset]])
        extends ProjectionManagementCommand
    final case class ManagementOperationException(op: Command, cause: Throwable) extends ProjectionManagementCommand

    final case class SetOffset[Offset](projectionId: ProjectionId, offset: Option[Offset], replyTo: ActorRef[Done])
        extends ProjectionManagementCommand
    final case class SetOffsetResult[Offset](replyTo: ActorRef[Done]) extends ProjectionManagementCommand

    final case class IsPaused(projectionId: ProjectionId, replyTo: ActorRef[Boolean])
        extends ProjectionManagementCommand
    final case class SetPaused(projectionId: ProjectionId, paused: Boolean, replyTo: ActorRef[Done])
        extends ProjectionManagementCommand
    final case class GetManagementStateResult(state: Option[ManagementState], replyTo: ActorRef[Boolean])
        extends ProjectionManagementCommand
    final case class SetPausedResult(replyTo: ActorRef[Done]) extends ProjectionManagementCommand
  }

  /**
   * Java API: creates a ProjectionBehavior for the passed projections.
   */
  def create[Envelope](projectionFactory: Projection[Envelope]): Behavior[ProjectionBehavior.Command] =
    apply(projectionFactory)

  /**
   * Java API: The `ProjectionBehavior` and its `Projection` can be stopped with this message.
   */
  def stopMessage(): Command = Stop

  /**
   * Scala API: creates a ProjectionBehavior for the passed projections.
   */
  def apply[Envelope](projection: Projection[Envelope]): Behavior[ProjectionBehavior.Command] = {
    Behaviors.setup[Command] { ctx =>
      Behaviors.withStash[Command](1000) { stashBuffer =>
        ctx.log.info("Starting projection [{}]", projection.projectionId)
        projection.actorHandlerInit[Any].foreach { init =>
          val ref = ctx.spawnAnonymous(Behaviors.supervise(init.behavior).onFailure(SupervisorStrategy.restart))
          init.setActor(ref)
          ctx.log.debug2("Started actor handler [{}] for projection [{}]", ref, projection.projectionId)
        }
        val running = projection.run()(ctx.system)
        if (running.isInstanceOf[RunningProjectionManagement[_]])
          ProjectionManagement(ctx.system).register(projection.projectionId, ctx.self)
        new ProjectionBehavior[Nothing, Envelope](ctx, projection, stashBuffer).started(running)
      }
    }
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[projection] class ProjectionBehavior[Offset, Envelope](
    context: ActorContext[ProjectionBehavior.Command],
    projection: Projection[Envelope],
    stashBuffer: StashBuffer[ProjectionBehavior.Command]) {
  import ProjectionBehavior.Internal._
  import ProjectionBehavior._

  private def projectionId = projection.projectionId

  private def started(running: RunningProjection): Behavior[Command] =
    Behaviors
      .receiveMessagePartial[Command] {
        case Stop =>
          context.log.debug("Projection [{}] is being stopped", projectionId)
          val stoppedFut = running.stop()
          // we send a Stopped for whatever completes the Future
          // Success or Failure, doesn't matter, since the internal stream is by then stopped
          context.pipeToSelf(stoppedFut)(_ => Stopped)
          stopping()

        case getOffset: GetOffset[Offset] @unchecked =>
          running match {
            case mgmt: RunningProjectionManagement[Offset] @unchecked =>
              if (getOffset.projectionId == projectionId) {
                context.pipeToSelf(mgmt.getOffset()) {
                  case Success(offset) => GetOffsetResult(offset, getOffset.replyTo)
                  case Failure(exc)    => ManagementOperationException(getOffset, exc)
                }
              }
              Behaviors.same
            case _ => Behaviors.unhandled
          }

        case result: GetOffsetResult[Offset] @unchecked =>
          receiveGetOffsetResult(result)

        case setOffset: SetOffset[Offset] @unchecked =>
          running match {
            case mgmt: RunningProjectionManagement[Offset] @unchecked =>
              if (setOffset.projectionId == projectionId) {
                context.log.info2(
                  "Offset will be changed to [{}] for projection [{}]. The Projection will be restarted.",
                  setOffset.offset,
                  projectionId)
                context.pipeToSelf(running.stop())(_ => Stopped)
                settingOffset(setOffset, mgmt)
              } else {
                Behaviors.same // not for this projectionId
              }
            case _ => Behaviors.unhandled
          }

        case ManagementOperationException(op, exc) =>
          context.log.warn2("Operation [{}] failed with: {}", op, exc)
          Behaviors.same

        case isPaused: IsPaused =>
          running match {
            case mgmt: RunningProjectionManagement[_] =>
              if (isPaused.projectionId == projectionId) {
                context.pipeToSelf(mgmt.getManagementState()) {
                  case Success(state) => GetManagementStateResult(state, isPaused.replyTo)
                  case Failure(exc)   => ManagementOperationException(isPaused, exc)
                }
              }
              Behaviors.same
            case _ => Behaviors.unhandled
          }

        case GetManagementStateResult(state, replyTo) =>
          replyTo ! state.exists(_.paused)
          Behaviors.same

        case setPaused: SetPaused =>
          running match {
            case mgmt: RunningProjectionManagement[_] =>
              if (setPaused.projectionId == projectionId) {
                context.log.info2(
                  "Running state will be changed to [{}] for projection [{}].",
                  if (setPaused.paused) "paused" else "resumed",
                  projectionId)
                context.pipeToSelf(running.stop())(_ => Stopped)
                settingPaused(setPaused, mgmt)
              } else {
                Behaviors.same // not for this projectionId
              }
            case _ => Behaviors.unhandled
          }

      }
      .receiveSignal {
        case (_, PostStop | PreRestart) =>
          // Make sure it is stopped in case the actor is terminated/restarted in other ways
          // than with ProjectionBehavior.Stop message. Note that we can't wait in that case, so
          // it's always better to use ProjectionBehavior.Stop message.
          running.stop()
          Behaviors.same
      }

  private def settingOffset(
      setOffset: SetOffset[Offset],
      mgmt: RunningProjectionManagement[Offset]): Behavior[Command] =
    Behaviors.receiveMessage {
      case Stopped =>
        context.log.debug("Projection [{}] stopped", projectionId)

        context.pipeToSelf(mgmt.setOffset(setOffset.offset)) {
          case Success(_)   => SetOffsetResult(setOffset.replyTo)
          case Failure(exc) => ManagementOperationException(setOffset, exc)
        }

        Behaviors.same

      case SetOffsetResult(replyTo) =>
        context.log.info2(
          "Starting projection [{}] after setting offset to [{}]",
          projection.projectionId,
          setOffset.offset)
        val running = projection.run()(context.system)
        replyTo ! Done
        stashBuffer.unstashAll(started(running))

      case ManagementOperationException(op, exc) =>
        context.log.warn2("Operation [{}] failed.", op, exc)
        // start anyway, but no reply
        val running = projection.run()(context.system)
        stashBuffer.unstashAll(started(running))

      case other =>
        stashBuffer.stash(other)
        Behaviors.same
    }

  private def stopping(): Behavior[Command] =
    Behaviors.receiveMessage {
      case Stopped =>
        context.log.debug("Projection [{}] stopped", projectionId)
        Behaviors.stopped

      case other =>
        context.log.debug2("Projection [{}] is being stopped. Discarding [{}].", projectionId, other)
        Behaviors.unhandled
    }

  private def receiveGetOffsetResult(result: GetOffsetResult[Offset]): Behavior[Command] = {
    result.replyTo ! CurrentOffset(projectionId, result.offset)
    Behaviors.same
  }

  private def settingPaused(setPaused: SetPaused, mgmt: RunningProjectionManagement[_]): Behavior[Command] =
    Behaviors.receiveMessage {
      case Stopped =>
        context.log.debug("Projection [{}] stopped", projectionId)

        context.pipeToSelf(mgmt.setPaused(setPaused.paused)) {
          case Success(_)   => SetPausedResult(setPaused.replyTo)
          case Failure(exc) => ManagementOperationException(setPaused, exc)
        }

        Behaviors.same

      case SetPausedResult(replyTo) =>
        context.log.info2(
          "Starting projection [{}] in {} mode.",
          projection.projectionId,
          if (setPaused.paused) "paused" else "resumed")
        val running = projection.run()(context.system)
        replyTo ! Done
        stashBuffer.unstashAll(started(running))

      case ManagementOperationException(op, exc) =>
        context.log.warn2("Operation [{}] failed.", op, exc)
        // start anyway, but no reply
        val running = projection.run()(context.system)
        stashBuffer.unstashAll(started(running))

      case other =>
        stashBuffer.stash(other)
        Behaviors.same
    }
}
