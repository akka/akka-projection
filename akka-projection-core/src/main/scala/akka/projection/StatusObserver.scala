/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import akka.annotation.ApiMayChange

/**
 * Track status of a projection by implementing a `StatusObserver` and install it using
 * [[Projection.withStatusObserver]].
 */
@ApiMayChange
abstract class StatusObserver[-Envelope] {

  /**
   * Called when a projection is started.
   * Also called after the projection has been restarted.
   */
  def started(projectionId: ProjectionId): Unit

  /**
   * Called when a projection failed.
   *
   * The projection will be restarted unless the projection restart backoff settings
   * are configured with `max-restarts` limit.
   */
  def failed(projectionId: ProjectionId, cause: Throwable): Unit

  /**
   * Called when a projection is stopped.
   * Also called before the projection is restarted.
   */
  def stopped(projectionId: ProjectionId): Unit

  /**
   * Called as soon as an envelop is ready to be processed. The envelope processing may
   * not start immediately if grouping or batching are enabled.
   */
  def beforeProcess(projectionId: ProjectionId, envelope: Envelope): Unit

  /**
   * Invoked as soon as the projected information is readable by a separate thread (e.g
   * committed to database). It will not be invoked if the envelope is skipped or
   * handling fails.
   */
  def afterProcess(projectionId: ProjectionId, envelope: Envelope): Unit

  /**
   * Called when the corresponding offset has been stored.
   * It might not be called for each envelope.
   */
  def offsetProgress(projectionId: ProjectionId, env: Envelope): Unit

  /**
   * Called when processing of an envelope failed. The invocation of this method is not guaranteed
   * when the handler failure causes a stream failure (e.g. using a Flow-based handler or a recovery
   * strategy that immediately fails).
   *
   * From the `recoveryStrategy` and keeping track how many times `error` is called it's possible to derive
   * what next step will be; fail, skip, retry.
   */
  def error(
      projectionId: ProjectionId,
      env: Envelope,
      cause: Throwable,
      recoveryStrategy: HandlerRecoveryStrategy): Unit
}
