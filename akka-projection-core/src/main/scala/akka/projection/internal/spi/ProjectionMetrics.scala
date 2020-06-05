/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal.spi

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalStableApi
import akka.projection.ProjectionId

/**
 * INTERNAL API
 */
@InternalStableApi private[akka] trait ProjectionMetrics {

  /**
   * Must be invoked for each element received from the Source when the Source provides it. If possible,
   * invoke this method in a stream stage as close to the stream stage where you read from the wire to
   * measure the parsing, deserializing and other steps of the processing prior to the event handling.
   *
   * @param projectionId      the projection id
   * @param creationTimestamp if the element traversing the stream contains the creation time, provide it. Set
   *                          to 0L (zero) if the information is not available.
   * @param systemProvider    a `ClassicActorSystemProvider` for telemetry to extract/set data on the ActorSystem
   * @return a contextual object. This context must propagate with the elt.
   */
  def onProcessStart(
      projectionId: ProjectionId,
      creationTimestamp: Long,
      systemProvider: ClassicActorSystemProvider): AnyRef

  /**
   * Must be invoked for each element processed successfully only when the associated offset has been committed.
   *
   * @param projectionId the projection id
   * @param context      the contextual object returned by `onProcessStart`
   */
  def onProcessComplete(projectionId: ProjectionId, context: AnyRef): Unit

  /**
   * Must be invoked for each failure to process an element.
   *
   * @param projectionId the projection id
   * @param context      the contextual object returned by `onProcessStart`
   */
  def onProcessFailure(projectionId: ProjectionId, context: AnyRef): Unit

  /**
   * Must be invoked when the stream fails.
   *
   * @param projectionId   the projection id
   * @param cause          the cause of the failure
   * @param systemProvider a `ClassicActorSystemProvider` for telemetry to extract/set data on the ActorSystem
   */
  def onFailure(projectionId: ProjectionId, cause: Throwable, systemProvider: ClassicActorSystemProvider): Unit

}

/**
 * INTERNAL API
 */
@InternalStableApi
private[akka] object NoopProjectionMetrics extends ProjectionMetrics {

  override def onProcessStart(
      projectionId: ProjectionId,
      creationTimestamp: Long,
      systemProvider: ClassicActorSystemProvider): AnyRef = null

  override def onProcessComplete(projectionId: ProjectionId, context: AnyRef): Unit = {}

  override def onProcessFailure(projectionId: ProjectionId, context: AnyRef): Unit = {}

  override def onFailure(
      projectionId: ProjectionId,
      cause: Throwable,
      systemProvider: ClassicActorSystemProvider): Unit = {}

}
