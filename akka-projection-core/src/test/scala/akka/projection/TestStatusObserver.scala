/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import akka.actor.typed.ActorRef

object TestStatusObserver {
  sealed trait Status

  case object Started extends Status
  case object Failed extends Status
  case object Stopped extends Status

  final case class OffsetProgress[Envelope](env: Envelope) extends Status

  trait EnvelopeProgress[Envelope] extends Status
  final case class Before[Envelope](env: Envelope) extends EnvelopeProgress[Envelope]
  final case class After[Envelope](env: Envelope) extends EnvelopeProgress[Envelope]

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
    offsetProgressProbe: Option[ActorRef[TestStatusObserver.OffsetProgress[Envelope]]] = None,
    envelopeProgressProbe: Option[ActorRef[TestStatusObserver.EnvelopeProgress[Envelope]]] = None)
    extends StatusObserver[Envelope] {
  import TestStatusObserver._

  override def started(projectionId: ProjectionId): Unit = {
    if (lifecycle)
      probe ! Started
  }

  override def failed(projectionId: ProjectionId, cause: Throwable): Unit = {
    if (lifecycle)
      probe ! Failed
  }

  override def stopped(projectionId: ProjectionId): Unit = {
    if (lifecycle)
      probe ! Stopped
  }

  override def offsetProgress(projectionId: ProjectionId, env: Envelope): Unit = {
    offsetProgressProbe.foreach(_ ! OffsetProgress(env))
  }

  override def error(
      projectionId: ProjectionId,
      env: Envelope,
      cause: Throwable,
      recoveryStrategy: HandlerRecoveryStrategy): Unit = {
    probe ! Err(env, cause)
  }
}
