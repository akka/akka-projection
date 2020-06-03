/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import akka.actor.typed.ActorRef

object TestStatusObserver {
  sealed trait Status

  case object Started extends Status
  case object Restarted extends Status
  case object Stopped extends Status

  final case class Progress[Envelope](env: Envelope) extends Status

  final case class Err[Envelope](env: Envelope, cause: Throwable) extends Status {
    // don't include cause message in equals
    override def equals(obj: Any): Boolean = obj match {
      case Err(`env`, e) => e.getClass == cause.getClass
      case _             => false
    }

    override def hashCode(): Int = env.hashCode()
  }
}

class TestStatusObserver[Envelope](
    probe: ActorRef[TestStatusObserver.Status],
    lifecycle: Boolean = false,
    progressProbe: Option[ActorRef[TestStatusObserver.Progress[Envelope]]] = None)
    extends StatusObserver[Envelope] {
  import TestStatusObserver._

  override def started(projectionId: ProjectionId): Unit = {
    if (lifecycle)
      probe ! Started
  }

  override def restarted(projectionId: ProjectionId, cause: Throwable): Unit = {
    if (lifecycle)
      probe ! Restarted
  }

  override def stopped(projectionId: ProjectionId): Unit = {
    if (lifecycle)
      probe ! Stopped
  }

  override def progress(projectionId: ProjectionId, env: Envelope): Unit = {
    progressProbe.foreach(_ ! Progress(env))
  }

  override def error(
      projectionId: ProjectionId,
      env: Envelope,
      cause: Throwable,
      recoveryStrategy: HandlerRecoveryStrategy): Unit = {
    probe ! Err(env, cause)
  }
}
