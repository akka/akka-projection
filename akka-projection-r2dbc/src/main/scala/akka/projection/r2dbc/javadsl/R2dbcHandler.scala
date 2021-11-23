/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.javadsl

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.function.BiFunction

import akka.Done
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.projection.javadsl.HandlerLifecycle

object R2dbcHandler {

  /**
   * INTERNAL API
   */
  @InternalApi
  private class HandlerFunction[Envelope](handler: BiFunction[R2dbcSession, Envelope, CompletionStage[Done]])
      extends R2dbcHandler[Envelope] {
    override def process(session: R2dbcSession, envelope: Envelope): CompletionStage[Done] =
      handler.apply(session, envelope)
  }

  /**
   * Handler that can be defined from a simple function
   */
  def fromFunction[Envelope](
      handler: BiFunction[R2dbcSession, Envelope, CompletionStage[Done]]): R2dbcHandler[Envelope] =
    new HandlerFunction(handler)

}

/**
 * Implement this interface for the Envelope handler for R2DBC Projections.
 *
 * It can be stateful, with variables and mutable data structures. It is invoked by the `Projection` machinery one
 * envelope at a time and visibility guarantees between the invocations are handled automatically, i.e. no volatile or
 * other concurrency primitives are needed for managing the state.
 *
 * Supported error handling strategies for when processing an `Envelope` fails can be defined in configuration or using
 * the `withRecoveryStrategy` method of a `Projection` implementation.
 */
@ApiMayChange
abstract class R2dbcHandler[Envelope] extends HandlerLifecycle {

  /**
   * The `process` method is invoked for each `Envelope`. Each time a new `Connection` is passed with a new open
   * transaction. You can use `createStatement`, `update` and other methods provided by the [[R2dbcSession]]. The
   * results of several statements can be combined with `CompletionStage` composition (e.g. `thenCompose`). The
   * transaction will be automatically committed or rolled back when the returned `CompletionStage` is completed.
   *
   * One envelope is processed at a time. It will not be invoked with the next envelope until after this method returns
   * and the returned `CompletionStage` is completed.
   */
  @throws(classOf[Exception])
  def process(session: R2dbcSession, envelope: Envelope): CompletionStage[Done]

  /**
   * Invoked when the projection is starting, before first envelope is processed. Can be overridden to implement
   * initialization. It is also called when the `Projection` is restarted after a failure.
   */
  def start(): CompletionStage[Done] =
    CompletableFuture.completedFuture(Done)

  /**
   * Invoked when the projection has been stopped. Can be overridden to implement resource cleanup. It is also called
   * when the `Projection` is restarted after a failure.
   */
  def stop(): CompletionStage[Done] =
    CompletableFuture.completedFuture(Done)

}
