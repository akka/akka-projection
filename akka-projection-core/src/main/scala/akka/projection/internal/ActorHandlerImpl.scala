/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.annotation.InternalApi
import akka.projection.scaladsl.Handler

/**
 * INTERNAL API
 */
@InternalApi private[projection] abstract class ActorHandlerImpl[Envelope, T](override val behavior: Behavior[T])
    extends Handler[Envelope]
    with ActorHandlerInit[T] {

  private var actor: Option[ActorRef[T]] = None

  override private[projection] def setActor(ref: ActorRef[T]): Unit =
    actor = Some(ref)

  override final def process(envelope: Envelope): Future[Done] = {
    actor match {
      case Some(ref) =>
        process(envelope, ref)
      case None =>
        throw new IllegalStateException(
          "Actor not started, please report issue at " +
          "https://github.com/akka/akka-projection/issues")
    }
  }

  def process(envelope: Envelope, actor: ActorRef[T]): Future[Done]

}

/**
 * INTERNAL API
 */
@InternalApi private[projection] trait ActorHandlerInit[T] {

  /** INTERNAL API */
  @InternalApi private[projection] def behavior: Behavior[T]

  /** INTERNAL API */
  @InternalApi private[projection] def setActor(ref: ActorRef[T]): Unit
}
