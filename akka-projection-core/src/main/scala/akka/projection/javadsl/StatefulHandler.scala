/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.ApiMayChange

@ApiMayChange abstract class StatefulHandler[State, Envelope] extends Handler[Envelope] {

  private var state: CompletionStage[State] = _

  /**
   * Invoked to load the initial state when the projection is started or if previous `process` failed.
   */
  def initialState(): CompletionStage[State]

  /**
   * The `process` method is invoked for each `Envelope`.
   * One envelope is processed at a time. The returned `CompletionStage<State>` is to be completed when the processing
   * of the `envelope` has finished. It will not be invoked with the next envelope until after the returned
   * `CompletionStage` has been completed.
   *
   * The `state` is the completed value of the previously returned `CompletionStage<State>` or the `initialState`.
   * If the previously returned `CompletionStage<State>` failed it will call `initialState`
   * again and use that value.
   */
  @throws(classOf[Exception])
  def process(state: State, envelope: Envelope): CompletionStage[State]

  /**
   * Calls [[StatefulHandler.initialState]] when the projection is started.
   */
  final override def start(): CompletionStage[Done] = {
    state = initialState()
    state.thenApply(_ => Done)
  }

  /**
   * Calls `process(state, envelope)` with the completed value of the previously returned `CompletionStage<State>`
   * or the `initialState`. If the previously returned `CompletionStage<State>` failed it will call `initialState`
   * again and use that value.
   */
  final override def process(envelope: Envelope): CompletionStage[Done] = {
    val stateCompletableFuture = state.toCompletableFuture
    val newState =
      if (stateCompletableFuture.isDone)
        state
      else if (stateCompletableFuture.isCompletedExceptionally)
        initialState()
      else
        throw new IllegalStateException(
          "Process called before previous CompletionStage completed. " +
          "Did you share the same handler instance between several Projection instances? " +
          "Otherwise, please report issue at " +
          "https://github.com/akka/akka-projection/issues")

    state = newState.thenCompose(s => process(s, envelope))
    state.thenApply(_ => Done)
  }

}
