/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.annotation.InternalApi
import akka.projection.ProjectionContext

/**
 * INTERNAL API
 * @param groupSize is used only in GroupHandlerStrategies so a single context instance
 *                  can report that multiple envelopes were processed.
 */
@InternalApi private[projection] case class ProjectionContextImpl[Offset, Envelope] private (
    offset: Offset,
    envelope: Envelope,
    // TODO: make it a volatile var in the case class ??
    groupSize: Int,
    readyTimestampNanos: Long)
    extends ProjectionContext

/**
 * INTERNAL API
 */
@InternalApi private[projection] object ProjectionContextImpl {
  def apply[Offset, Envelope](offset: Offset, envelope: Envelope): ProjectionContextImpl[Offset, Envelope] =
    new ProjectionContextImpl(offset, envelope, groupSize = 1, readyTimestampNanos = System.nanoTime())
}
