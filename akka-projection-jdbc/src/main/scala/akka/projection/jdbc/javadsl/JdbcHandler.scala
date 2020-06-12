/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.javadsl

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

import akka.Done
import akka.projection.javadsl.HandlerLifecycle
import akka.projection.jdbc.JdbcSession

abstract class JdbcHandler[Envelope, S <: JdbcSession] extends HandlerLifecycle {

  def process(session: S, envelope: Envelope): Unit

  /**
   * Invoked when the projection is starting, before first envelope is processed.
   * Can be overridden to implement initialization. It is also called when the `Projection`
   * is restarted after a failure.
   */
  override def start(): CompletionStage[Done] = CompletableFuture.completedFuture(Done)

  /**
   * Invoked when the projection has been stopped. Can be overridden to implement resource
   * cleanup. It is also called when the `Projection` is restarted after a failure.
   */
  override def stop(): CompletionStage[Done] = CompletableFuture.completedFuture(Done)
}
