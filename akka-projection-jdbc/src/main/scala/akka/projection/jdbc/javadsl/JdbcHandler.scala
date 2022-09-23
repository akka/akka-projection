/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.javadsl

import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.projection.jdbc.JdbcHandlerLifecycle
import akka.projection.jdbc.JdbcSession

import java.util.function.BiConsumer

@ApiMayChange
object JdbcHandler {

  /**
   * Handler that can be defined from a simple function
   *
   * INTERNAL API
   */
  @InternalApi
  private class HandlerFunction[Envelope, S <: JdbcSession](handler: BiConsumer[S, Envelope])
      extends JdbcHandler[Envelope, S] {
    override def process(session: S, envelope: Envelope): Unit = handler.accept(session, envelope)
  }

  @Deprecated
  @deprecated("Use the similar method with Java functional interface as a parameter", "1.2.1")
  def fromFunction[Envelope, S <: JdbcSession](handler: (S, Envelope) => Unit): JdbcHandler[Envelope, S] =
    new HandlerFunction((s, env) => handler(s, env))

  def fromFunction[Envelope, S <: JdbcSession](handler: BiConsumer[S, Envelope]): JdbcHandler[Envelope, S] =
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
@ApiMayChange
abstract class JdbcHandler[Envelope, S <: JdbcSession] extends JdbcHandlerLifecycle {

  /**
   * The `process` method is invoked for each `Envelope`. Each time a new [[JdbcSession]] is passed with a new open transaction.
   * It's allowed to run any blocking JDBC operation inside this method.
   *
   * One envelope is processed at a time. It will not be invoked with the next envelope until after this method returns.
   */
  @throws(classOf[Exception])
  def process(session: S, envelope: Envelope): Unit

}
