/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.annotation.InternalApi
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.StatusObserver

/**
 * INTERNAL API
 */
@InternalApi private[projection] object NoopStatusObserver extends StatusObserver[Any] {

  // Java access
  def getInstance[Envelope]: StatusObserver[Envelope] = NoopStatusObserver

  override def started(projectionId: ProjectionId): Unit = ()

  override def failed(projectionId: ProjectionId, cause: Throwable): Unit = ()

  override def stopped(projectionId: ProjectionId): Unit = ()

  override def beforeProcess(projectionId: ProjectionId, envelope: Any): Unit = ()

  override def afterProcess(projectionId: ProjectionId, envelope: Any): Unit = ()

  override def offsetProgress(projectionId: ProjectionId, env: Any): Unit = ()

  override def error(
      projectionId: ProjectionId,
      env: Any,
      cause: Throwable,
      recoveryStrategy: HandlerRecoveryStrategy): Unit = ()
}
