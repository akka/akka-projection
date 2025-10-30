/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.jdbc

trait JdbcHandlerLifecycle {

  /**
   * Invoked when the projection is starting, before first envelope is processed.
   * Can be overridden to implement initialization.
   */
  def start(): Unit = ()

  /**
   * Invoked when the projection has been stopped. Can be overridden to implement resource
   * cleanup.
   */
  def stop(): Unit = ()
}
