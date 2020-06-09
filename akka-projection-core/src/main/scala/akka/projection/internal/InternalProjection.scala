/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.annotation.InternalApi
import akka.projection.Projection

/**
 * INTERNAL API
 */
@InternalApi
private[projection] trait InternalProjection[Offset, Envelope] extends Projection[Envelope] {

  /**
   * INTERNAL API
   */
  @InternalApi
  private[projection] def offsetStrategy: OffsetStrategy
}
