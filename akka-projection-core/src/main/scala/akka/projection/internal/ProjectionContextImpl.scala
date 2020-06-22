/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.annotation.InternalApi
import akka.projection.ProjectionContext

/**
 * INTERNAL API
 */
@InternalApi private[projection] case class ProjectionContextImpl[Offset, Envelope] private (
    offset: Offset,
    envelope: Envelope)
    extends ProjectionContext

/**
 * INTERNAL API
 */
@InternalApi private[projection] object ProjectionContextImpl {
  def apply[Offset, Envelope](offset: Offset, envelope: Envelope): ProjectionContextImpl[Offset, Envelope] =
    new ProjectionContextImpl(offset, envelope)

  def apply[Offset, Envelope](
      envelope: Envelope,
      offsetExtractor: Envelope => Offset): ProjectionContextImpl[Offset, Envelope] =
    ProjectionContextImpl(offsetExtractor(envelope), envelope)
}
