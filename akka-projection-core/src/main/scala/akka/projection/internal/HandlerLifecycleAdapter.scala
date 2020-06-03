/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.Future

import scala.compat.java8.FutureConverters._
import akka.Done
import akka.annotation.InternalApi
import akka.projection.javadsl
import akka.projection.scaladsl

/**
 * INTERNAL API: Adapter from javadsl.HandlerLifecycle to scaladsl.HandlerLifecycle
 */
@InternalApi
private[akka] class HandlerLifecycleAdapter(delegate: javadsl.HandlerLifecycle) extends scaladsl.HandlerLifecycle {

  /**
   * Invoked when the projection is starting, before first envelope is processed.
   * Can be overridden to implement initialization. It is also called when the `Projection`
   * is restarted after a failure.
   */
  override def start(): Future[Done] =
    delegate.start().toScala

  /**
   * Invoked when the projection has been stopped. Can be overridden to implement resource
   * cleanup. It is also called when the `Projection` is restarted after a failure.
   */
  override def stop(): Future[Done] =
    delegate.stop().toScala
}
