/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.Future
import scala.util.control.NonFatal

import akka.Done
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi

@ApiMayChange object Handler {

  /** Handler that can be define from a simple function */
  private class HandlerFunction[Envelope](handler: Envelope => Future[Done]) extends Handler[Envelope] {
    override def process(envelope: Envelope): Future[Done] = handler(envelope)
  }

  def apply[Envelope](handler: Envelope => Future[Done]): Handler[Envelope] = new HandlerFunction(handler)
}

/**
 * Implement this interface for the Envelope handler in the `Projection`. Some projections
 * may have more specific handler types.
 *
 * It can be stateful, with variables and mutable data structures.
 * It is invoked by the `Projection` machinery one envelope at a time and visibility
 * guarantees between the invocations are handled automatically, i.e. no volatile or
 * other concurrency primitives are needed for managing the state.
 *
 * Error handling strategy for when processing an `Envelope` fails can be defined in the supported
 * [[HandlerRecoveryStrategyHandler]] in settings or passed to the [[akka.projection.Projection]].
 */
@ApiMayChange trait Handler[Envelope] extends HandlerLifecycle {

  /**
   * The `process` method is invoked for each `Envelope`.
   * One envelope is processed at a time. The returned `Future` is to be completed when the processing
   * of the `envelope` has finished. It will not be invoked with the next envelope until after the returned
   * `Future` has been completed.
   */
  def process(envelope: Envelope): Future[Done]

}

@ApiMayChange trait HandlerLifecycle {

  /**
   * Invoked when the projection is starting, before first envelope is processed.
   * Can be overridden to implement initialization. It is also called when the `Projection`
   * is restarted after a failure.
   */
  def start(): Future[Done] =
    Future.successful(Done)

  /**
   * Invoked when the projection has been stopped. Can be overridden to implement resource
   * cleanup. It is also called when the `Projection` is restarted after a failure.
   */
  def stop(): Future[Done] =
    Future.successful(Done)

  /** INTERNAL API */
  @InternalApi private[akka] def tryStart(): Future[Done] = {
    try {
      start()
    } catch {
      case NonFatal(exc) => Future.failed(exc) // in case the call throws
    }
  }

  /** INTERNAL API */
  @InternalApi private[akka] def tryStop(): Future[Done] = {
    try {
      stop()
    } catch {
      case NonFatal(exc) => Future.failed(exc) // in case the call throws
    }
  }

}
