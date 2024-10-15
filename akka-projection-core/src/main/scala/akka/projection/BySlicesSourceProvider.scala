/*
 * Copyright (C) 2021-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

/**
 * Implemented by `EventSourcedProvider` and `DurableStateSourceProvider`.
 */
trait BySlicesSourceProvider {
  def minSlice: Int
  def maxSlice: Int
}
