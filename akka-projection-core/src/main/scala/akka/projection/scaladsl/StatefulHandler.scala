/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import akka.Done

abstract class StatefulHandler[State, Envelope](implicit ec: ExecutionContext) extends Handler[Envelope] {

  private var state: Future[State] = _

  /**
   * Invoked to load the initial state when the projection is started or if previous `process` failed.
   */
  def initialState(): Future[State]

  /**
   * The `process` method is invoked for each `Envelope`.
   * One envelope is processed at a time. The returned `Future[State]` is to be completed when the processing
   * of the `envelope` has finished. It will not be invoked with the next envelope until after the returned
   * `Future` has been completed.
   *
   * The `state` is the completed value of the previously returned `Future[State]` or the `initialState`.
   * If the previously returned `Future[State]` failed it will call `initialState`
   * again and use that value.
   */
  def process(state: State, envelope: Envelope): Future[State]

  /**
   * Calls [[StatefulHandler.initialState]] when the projection is started.
   */
  final override def start(): Future[Done] = {
    state = initialState()
    state.map(_ => Done)
  }

  /**
   * Calls `process(state, envelope)` with the completed value of the previously returned `Future[State]`
   * or the `initialState`. If the previously returned `Future[State]` failed it will call `initialState`
   * again and use that value.
   */
  final override def process(envelope: Envelope): Future[Done] = {
    val newState = state.value match {
      case Some(Success(_)) => state
      case Some(Failure(_)) => initialState()
      case None =>
        throw new IllegalStateException(
          "Process called before previous Future completed. " +
          "Did you share the same handler instance between several Projection instances? " +
          "Otherwise, please report issue at " +
          "https://github.com/akka/akka-projection/issues")
    }
    state = newState.flatMap(s => process(s, envelope))
    state.map(_ => Done)
  }

}
