/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import java.util.function.Supplier

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object ProjectionBehavior {

  sealed trait Command
  object Stop extends Command
  private object Stopped extends Command

  /**
   * Java API: creates a ProjectionBehavior for the passed projections.
   *
   * Projections can only be started once, therefore a [[Supplier]] is required. On restart, a new projection instance will be created.
   */
  def create[Envelope](projectionFactory: Projection[Envelope]): Behavior[ProjectionBehavior.Command] =
    apply(projectionFactory)

  /**
   * Java API: The top message used to stop the projection.
   */
  def stopMessage(): Command = Stop

  /**
   * Scala API: creates a ProjectionBehavior for the passed projections.
   *
   * Projections can only be started once, therefore a factory function [[() => Projection]] is required.
   * On restart, a new projection instance will be created.
   */
  def apply[Envelope](projection: Projection[Envelope]): Behavior[ProjectionBehavior.Command] = {

    def started(projection: Projection[Envelope]): Behavior[Command] =
      Behaviors.setup[Command] { ctx =>

        ctx.log.info("Starting projection [{}]", projection.projectionId)
        val running = projection.run()(ctx.system)

        Behaviors.receiveMessagePartial {
          case Stop =>
            ctx.log.debug("Projection [{}] is being stopped", projection.projectionId)
            val stoppedFut = running.stop()(ctx.executionContext)
            // we send a Stopped for whatever completes the Future
            // Success or Failure, doesn't matter, since the internal stream is by then stopped
            ctx.pipeToSelf(stoppedFut)(_ => Stopped)
            stopping(projection.projectionId)
        }
      }

    def stopping(projectionId: ProjectionId): Behavior[Command] =
      Behaviors.receive {
        case (ctx, Stopped) =>
          ctx.log.debug("Projection [{}] stopped", projectionId)
          Behaviors.stopped

        case (ctx, Stop) =>
          ctx.log.debug("Projection [{}] is already being stopped", projectionId)
          Behaviors.same
      }

    // starting by default
    started(projection)

  }

}
