/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[projection] trait InternalProjection {

  /**
   * INTERNAL API
   */
  @InternalApi
  private[projection] def offsetStrategy: OffsetStrategy
}
