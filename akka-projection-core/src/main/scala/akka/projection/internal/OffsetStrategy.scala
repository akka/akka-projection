/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.annotation.InternalApi
import akka.projection.HandlerRecoveryStrategy
import akka.projection.StrictRecoveryStrategy
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.HandlerLifecycle
import akka.stream.scaladsl.FlowWithContext

/**
 * INTERNAL API
 */
@InternalApi
private[projection] sealed trait OffsetStrategy

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class AtMostOnce(recoveryStrategy: Option[StrictRecoveryStrategy] = None)
    extends OffsetStrategy

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class ExactlyOnce(recoveryStrategy: Option[HandlerRecoveryStrategy] = None)
    extends OffsetStrategy

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class AtLeastOnce(
    afterEnvelopes: Option[Int] = None,
    orAfterDuration: Option[FiniteDuration] = None,
    recoveryStrategy: Option[HandlerRecoveryStrategy] = None)
    extends OffsetStrategy

/**
 * INTERNAL API
 */
@InternalApi
private[projection] sealed trait HandlerStrategy[Envelope] {
  def lifecycle: HandlerLifecycle

  def actorHandlerInit[T]: Option[ActorHandlerInit[T]]
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class SingleHandlerStrategy[Envelope](handler: Handler[Envelope])
    extends HandlerStrategy[Envelope] {
  override def lifecycle: HandlerLifecycle = handler

  override def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = handler match {
    case init: ActorHandlerInit[T] @unchecked => Some(init)
    case _                                    => None
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class GroupedHandlerStrategy[Envelope](
    handler: Handler[immutable.Seq[Envelope]],
    afterEnvelopes: Option[Int] = None,
    orAfterDuration: Option[FiniteDuration] = None)
    extends HandlerStrategy[Envelope] {
  override def lifecycle: HandlerLifecycle = handler

  override def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = handler match {
    case init: ActorHandlerInit[T] @unchecked => Some(init)
    case _                                    => None
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class FlowHandlerStrategy[Envelope](
    flowCtx: FlowWithContext[Envelope, Envelope, Done, Envelope, _])
    extends HandlerStrategy[Envelope] {
  override val lifecycle: HandlerLifecycle = new HandlerLifecycle {}

  override def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = None
}
