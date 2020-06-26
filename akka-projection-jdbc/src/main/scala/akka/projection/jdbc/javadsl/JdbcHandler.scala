/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.javadsl

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

import akka.Done
import akka.projection.javadsl.HandlerLifecycle
import akka.projection.jdbc.JdbcSession

object JdbcHandler {

  /** Handler that can be defined from a simple function */
  private class HandlerFunction[Envelope, S <: JdbcSession](handler: (S, Envelope) => Unit)
      extends JdbcHandler[Envelope, S] {
    override def process(session: S, envelope: Envelope): Unit = handler(session, envelope)
  }

  def fromFunction[Envelope, S <: JdbcSession](handler: (S, Envelope) => Unit): JdbcHandler[Envelope, S] =
    new HandlerFunction(handler)
}

/**
 * Implement this interface for the Envelope handler for  Jdbc Projections.
 *
 * It can be stateful, with variables and mutable data structures.
 * It is invoked by the `Projection` machinery one envelope at a time and visibility
 * guarantees between the invocations are handled automatically, i.e. no volatile or
 * other concurrency primitives are needed for managing the state.
 *
 * Supported error handling strategies for when processing an `Envelope` fails can be
 * defined in configuration or using the `withRecoveryStrategy` method of a `Projection`
 * implementation.
 */
abstract class JdbcHandler[Envelope, S <: JdbcSession] extends HandlerLifecycle {

  /**
   * The `process` method is invoked for each `Envelope`. Each time a new [[JdbcSession]] is passed with a new open transaction.
   * It's allowed to run any blocking JDBC operation inside this method.
   *
   * One envelope is processed at a time. It will not be invoked with the next envelope until after this method returns.
   */
  @throws(classOf[Exception])
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
