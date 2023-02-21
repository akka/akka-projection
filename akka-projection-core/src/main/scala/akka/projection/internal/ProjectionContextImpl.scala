/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
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
    externalContext: AnyRef,
    groupSize: Int)
    extends ProjectionContext {

  /**
   * scala3 makes `copy` private
   */
  def withGroupSize(groupSize: Int) = copy(groupSize = groupSize)
}

/**
 * INTERNAL API
 */
@InternalApi private[projection] object ProjectionContextImpl {
  def apply[Offset, Envelope](
      offset: Offset,
      envelope: Envelope,
      externalContext: AnyRef): ProjectionContextImpl[Offset, Envelope] =
    new ProjectionContextImpl(offset, envelope, externalContext, groupSize = 1)
}
