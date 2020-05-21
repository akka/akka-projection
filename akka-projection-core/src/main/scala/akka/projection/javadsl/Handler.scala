/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.ApiMayChange

@ApiMayChange
object Handler {

  /** Handler that can be define from a simple function */
  private class HandlerFunction[Envelope](handler: Envelope => CompletionStage[Done]) extends Handler[Envelope] {
    override def process(envelope: Envelope): CompletionStage[Done] = handler(envelope)
  }

  def fromFunction[Envelope](handler: Envelope => CompletionStage[Done]): Handler[Envelope] =
    new HandlerFunction(handler)
}

/**
 * Implement this interface for the Envelope handler in the `Projection`. Some projections
 * may have more specific handler types.
 *
 * It can be stateful, with variables and mutable data structures.
 * It is invoked by the `Projection` machinery one envelope at a time and visibility
 * guarantees between the invocations are handled automatically, i.e. no volatile or
 * other concurrency primitives are needed for managing the state.
 */
@ApiMayChange
abstract class Handler[Envelope] extends HandlerLifecycle {

  /**
   * The `process` method is invoked for each `Envelope`.
   * One envelope is processed at a time. The returned `CompletionStage` is to be completed when the processing
   * of the `envelope` has finished. It will not be invoked with the next envelope until after the returned
   * `CompletionStage` has been completed.
   */
  def process(envelope: Envelope): CompletionStage[Done]

  /**
   * Invoked when the projection is starting, before first envelope is processed.
   * Can be overridden to implement initialization. It is also called when the `Projection`
   * is restarted after a failure.
   */
  def start(): CompletionStage[Done] =
    CompletableFuture.completedFuture(Done)

  /**
   * Invoked when the projection has been stopped. Can be overridden to implement resource
   * cleanup. It is also called when the `Projection` is restarted after a failure.
   */
  def stop(): CompletionStage[Done] =
    CompletableFuture.completedFuture(Done)
}

@ApiMayChange trait HandlerLifecycle {

  /**
   * Invoked when the projection is starting, before first envelope is processed.
   * Can be overridden to implement initialization.
   */
  def start(): CompletionStage[Done]

  /**
   * Invoked when the projection has been stopped. Can be overridden to implement resource
   * cleanup.
   */
  def stop(): CompletionStage[Done]
}
