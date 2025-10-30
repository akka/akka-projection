/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.internal

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.annotation.InternalApi
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionContext
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
private[projection] final case class OffsetStoredByHandler(recoveryStrategy: Option[HandlerRecoveryStrategy] = None)
    extends OffsetStrategy

/**
 * INTERNAL API
 */
@InternalApi
private[projection] sealed trait HandlerStrategy {
  def recreateHandlerOnNextAccess(): Unit

  def lifecycle: HandlerLifecycle

  def actorHandlerInit[T]: Option[ActorHandlerInit[T]]
}

@InternalApi
private[projection] sealed abstract class FunctionHandlerStrategy[Envelope](handlerFactory: () => Handler[Envelope])
    extends HandlerStrategy {
  @volatile private var _handler: Option[Handler[Envelope]] = None
  @volatile private var _recreateHandlerOnNextAccess = true

  def recreateHandlerOnNextAccess(): Unit =
    _recreateHandlerOnNextAccess = true

  /**
   * Current handler instance, or lazy creation of it.
   */
  def handler(): Handler[Envelope] = {
    _handler match {
      case Some(h: Handler[Any] @unchecked) if !_recreateHandlerOnNextAccess => h
      case _ =>
        createHandler()
        _recreateHandlerOnNextAccess = false
        _handler.get
    }
  }

  private def createHandler(): Unit = {
    val newHandler = handlerFactory()
    (_handler, newHandler) match {
      case (Some(h1: ActorHandlerInit[Any] @unchecked), h2: ActorHandlerInit[Any] @unchecked) =>
        // use same actor in new handler
        h2.setActor(h1.getActor())
      case _ =>
    }
    _handler = Some(newHandler)
  }

  override def lifecycle: HandlerLifecycle = handler()

  override def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = handler() match {
    case init: ActorHandlerInit[T] @unchecked => Some(init)
    case _                                    => None
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class SingleHandlerStrategy[Envelope](handlerFactory: () => Handler[Envelope])
    extends FunctionHandlerStrategy[Envelope](handlerFactory)

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class GroupedHandlerStrategy[Envelope](
    handlerFactory: () => Handler[immutable.Seq[Envelope]],
    afterEnvelopes: Option[Int] = None,
    orAfterDuration: Option[FiniteDuration] = None)
    extends FunctionHandlerStrategy[immutable.Seq[Envelope]](handlerFactory)

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class FlowHandlerStrategy[Envelope](
    flowCtx: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _])
    extends HandlerStrategy {

  override def recreateHandlerOnNextAccess(): Unit = ()

  override val lifecycle: HandlerLifecycle = new HandlerLifecycle {}

  override def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = None
}
