/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import akka.annotation.ApiMayChange

@ApiMayChange trait R2dbcHandlerLifecycle {

  /**
   * Invoked when the projection is starting, before first envelope is processed. Can be overridden to implement
   * initialization.
   */
  def start(): Unit = ()

  /**
   * Invoked when the projection has been stopped. Can be overridden to implement resource cleanup.
   */
  def stop(): Unit = ()
}
