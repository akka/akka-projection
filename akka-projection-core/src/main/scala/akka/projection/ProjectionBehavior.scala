/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.StashBuffer
import akka.annotation.InternalApi
import akka.projection.scaladsl.ProjectionManagement

object ProjectionBehavior {

  sealed trait Command
  object Stop extends Command // FIXME replyTo Done

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] object Internal {

    object Stopped extends Command

    sealed trait OffsetManagementCommand extends Command
    final case class GetOffset[Offset](projectionId: ProjectionId, replyTo: ActorRef[CurrentOffset[Offset]])
        extends OffsetManagementCommand
    final case class CurrentOffset[Offset](projectionId: ProjectionId, offset: Option[Offset])
    final case class GetOffsetResult[Offset](offset: Option[Offset], replyTo: ActorRef[CurrentOffset[Offset]])
        extends OffsetManagementCommand
    final case class OffsetOperationException(op: Command, cause: Throwable) extends OffsetManagementCommand

    final case class SetOffset[Offset](projectionId: ProjectionId, offset: Option[Offset], replyTo: ActorRef[Done])
        extends OffsetManagementCommand
    final case class SetOffsetResult[Offset](replyTo: ActorRef[Done]) extends OffsetManagementCommand
  }

  /**
   * Java API: creates a ProjectionBehavior for the passed projections.
   */
  def create[Envelope](projectionFactory: Projection[Envelope]): Behavior[ProjectionBehavior.Command] =
    apply(projectionFactory)

  /**
   * Java API: The top message used to stop the projection.
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
          ctx.log.debug("Started actor handler [{}] for projection [{}]", ref, projection.projectionId)
        }
        val running = projection.run()(ctx.system)
        if (running.isInstanceOf[ProjectionOffsetManagement[_]])
          ProjectionManagement(ctx.system).register(projection.projectionId, ctx.self)
        new ProjectionBehavior(ctx, projection, stashBuffer).started(running)
      }
    }
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ProjectionBehavior[Offset, Envelope](
    context: ActorContext[ProjectionBehavior.Command],
    projection: Projection[Envelope],
    stashBuffer: StashBuffer[ProjectionBehavior.Command]) {
  import ProjectionBehavior.Internal._
  import ProjectionBehavior._

  private def projectionId = projection.projectionId

  private def started(running: RunningProjection): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case Stop =>
        context.log.debug("Projection [{}] is being stopped", projectionId)
        val stoppedFut = running.stop()
        // we send a Stopped for whatever completes the Future
        // Success or Failure, doesn't matter, since the internal stream is by then stopped
        context.pipeToSelf(stoppedFut)(_ => Stopped)
        stopping()

      case getOffset: GetOffset[Offset] =>
        running match {
          case mgmt: ProjectionOffsetManagement[Offset] =>
            if (getOffset.projectionId == projectionId) {
              context.pipeToSelf(mgmt.getOffset()) {
                case Success(offset) => GetOffsetResult(offset, getOffset.replyTo)
                case Failure(exc)    => OffsetOperationException(getOffset, exc)
              }
            }
            Behaviors.same
          case _ => Behaviors.unhandled
        }

      case result: GetOffsetResult[Offset] =>
        receiveGetOffsetResult(result)

      case setOffset: SetOffset[Offset] =>
        running match {
          case mgmt: ProjectionOffsetManagement[Offset] =>
            if (setOffset.projectionId == projectionId) {
              context.log.debug(
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

      case OffsetOperationException(op, exc) =>
        context.log.warn("Operation [{}] failed with: {}", op, exc)
        Behaviors.same

    }

  private def settingOffset(setOffset: SetOffset[Offset], mgmt: ProjectionOffsetManagement[Offset]): Behavior[Command] =
    Behaviors.receiveMessage {
      case Stopped =>
        context.log.debug("Projection [{}] stopped", projectionId)

        context.pipeToSelf(mgmt.setOffset(setOffset.offset)) {
          case Success(_)   => SetOffsetResult(setOffset.replyTo)
          case Failure(exc) => OffsetOperationException(setOffset, exc)
        }

        Behaviors.same

      case SetOffsetResult(replyTo) =>
        context.log.info(
          "Starting projection [{}] after setting offset to [{}]",
          projection.projectionId,
          setOffset.offset)
        val running = projection.run()(context.system)
        replyTo ! Done
        stashBuffer.unstashAll(started(running))

      case OffsetOperationException(op, exc) =>
        context.log.warn("Operation [{}] failed.", op, exc)
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
        context.log.debug("Projection [{}] is being stopped. Discarding [{}].", projectionId, other)
        Behaviors.unhandled
    }

  private def receiveGetOffsetResult(result: GetOffsetResult[Offset]): Behavior[Command] = {
    result.replyTo ! CurrentOffset(projectionId, result.offset)
    Behaviors.same
  }
}
