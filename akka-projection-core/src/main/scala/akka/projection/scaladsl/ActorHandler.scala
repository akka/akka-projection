/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.util.Try

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.projection.ProjectionId
import akka.projection.internal.ActorHandlerImpl

object ActorHandler {

  def apply[Envelope, EnvelopeMessage](
      projectionId: ProjectionId,
      behavior: Behavior[_ >: EnvelopeMessage],
      envelopeMessage: (Envelope, ActorRef[Try[Done]]) => EnvelopeMessage)(
      implicit system: ActorSystem[_]): Handler[Envelope] =
    new ActorHandlerImpl(projectionId, behavior, envelopeMessage)

}
