/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

/**
 * Track status of a projection by implementing a `StatusObserver` and install it using
 * [[Projection.withStatusObserver]].
 */
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
   * Called when the envelope has been processed and corresponding offset stored.
   * It might not be called for each envelope.
   */
  def progress(projectionId: ProjectionId, env: Envelope): Unit

  /**
   * Called when processing of an envelope failed.
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
