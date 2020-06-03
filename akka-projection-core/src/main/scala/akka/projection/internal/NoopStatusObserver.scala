/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.annotation.InternalApi
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.StatusObserver

/**
 * INTERNAL API
 */
@InternalApi private[akka] object NoopStatusObserver extends StatusObserver[Any] {

  // Java access
  def getInstance[Envelope]: StatusObserver[Envelope] = NoopStatusObserver

  def started(projectionId: ProjectionId): Unit = ()

  def restarted(projectionId: ProjectionId): Unit = ()

  def stopped(projectionId: ProjectionId): Unit = ()

  def progress(projectionId: ProjectionId, env: Any): Unit = ()

  override def error(
      projectionId: ProjectionId,
      env: Any,
      cause: Throwable,
      recoveryStrategy: HandlerRecoveryStrategy): Unit = ()
}
