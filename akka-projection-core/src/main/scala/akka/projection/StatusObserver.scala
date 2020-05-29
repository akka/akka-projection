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
   */
  def started(projectionId: ProjectionId): Unit

  /**
   * Called when a projection is restarted due to failure.
   *
   * The `restartCount` starts at 1 for the first restart after failure and is incremented for each restart that
   * also failed. The counter is reset when at least one envelope has been processed successfully.
   */
  def restarted(projectionId: ProjectionId, restartCount: Int): Unit

  /**
   * Called when a projection is stopped.
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
   * The `errorCount` starts at 1 for the first error and is incremented for each retry that also failed.
   * It is only a counter for the specific envelope, not a total number of errors.
   *
   * From the `recoveryStrategy` and the `errorCount` it's possible to derive what next step will be;
   * fail, skip, retry.
   */
  def error(
      projectionId: ProjectionId,
      env: Envelope,
      cause: Throwable,
      errorCount: Int,
      recoveryStrategy: HandlerRecoveryStrategy): Unit
}
