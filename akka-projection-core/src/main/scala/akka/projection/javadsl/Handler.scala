/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.ApiMayChange
import akka.projection.HandlerRecovery

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
abstract class Handler[Envelope] extends HandlerRecovery[Envelope] {

  /**
   * The `process` method is invoked for each `Envelope`.
   * One envelope is processed at a time. The returned `CompletionStage` is to be completed when the processing
   * of the `envelope` has finished. It will not be invoked with the next envelope until after the returned
   * `CompletionStage` has been completed.
   */
  def process(envelope: Envelope): CompletionStage[Done]
}
