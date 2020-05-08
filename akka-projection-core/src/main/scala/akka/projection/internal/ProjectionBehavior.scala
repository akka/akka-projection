/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.projection.Projection
import akka.projection.ProjectionId

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object ProjectionBehavior {

  sealed trait Command
  object Stop extends Command
  object Stopped extends Command

  private[akka] def apply[Envelope](projectionFactory: () => Projection[Envelope]) = {

    def started(projection: Projection[Envelope]): Behavior[Command] =
      Behaviors.setup[Command] { ctx =>

        ctx.log.info("Starting projection [{}]", projection.projectionId)
        projection.run()(ctx.system)

        Behaviors.receiveMessagePartial {
          case Stop =>
            val stoppedFut = projection.stop()(ctx.executionContext)
            ctx.log.debug("Projection [{}] is being stopped", projection.projectionId)
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
    started(projectionFactory())

  }

}
