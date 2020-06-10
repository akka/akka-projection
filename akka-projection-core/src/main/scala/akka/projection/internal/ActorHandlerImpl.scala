/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.AskPattern._
import akka.annotation.InternalApi
import akka.projection.scaladsl.Handler
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalApi private[projection] class ActorHandlerImpl[Envelope, EnvelopeMessage](
    override val behavior: Behavior[EnvelopeMessage],
    envelopeMessage: (Envelope, ActorRef[Try[Done]]) => EnvelopeMessage)(implicit system: ActorSystem[_])
    extends Handler[Envelope]
    with ActorHandlerInit[EnvelopeMessage] {
  import system.executionContext
  private implicit val timeout: Timeout = 10.seconds // FIXME config

  private var actor: Option[ActorRef[EnvelopeMessage]] = None

  override private[projection] def setActor(ref: ActorRef[EnvelopeMessage]): Unit =
    actor = Some(ref)

  override def process(envelope: Envelope): Future[Done] = {
    actor match {
      case Some(ref) =>
        ref.ask[Try[Done]](replyTo => envelopeMessage(envelope, replyTo)).map {
          case Success(_)   => Done
          case Failure(exc) => throw exc
        }
      case None =>
        throw new IllegalStateException(
          "Actor not started, please report issue at " +
          "https://github.com/akka/akka-projection/issues")
    }
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[projection] trait ActorHandlerInit[M] {

  /** INTERNAL API */
  @InternalApi private[projection] def behavior: Behavior[M]

  /** INTERNAL API */
  @InternalApi private[projection] def setActor(ref: ActorRef[M]): Unit
}
