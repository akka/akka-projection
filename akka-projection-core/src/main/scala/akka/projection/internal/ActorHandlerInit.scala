/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.internal

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[projection] trait ActorHandlerInit[T] {
  private var actor: Option[ActorRef[T]] = None

  /** INTERNAL API */
  @InternalApi private[projection] def behavior: Behavior[T]

  /** INTERNAL API */
  final private[projection] def setActor(ref: ActorRef[T]): Unit =
    actor = Some(ref)

  /** INTERNAL API */
  final private[projection] def getActor(): ActorRef[T] = {
    actor match {
      case Some(ref) => ref
      case None =>
        throw new IllegalStateException(
          "Actor not started, please report issue at " +
          "https://github.com/akka/akka-projection/issues")
    }
  }
}
