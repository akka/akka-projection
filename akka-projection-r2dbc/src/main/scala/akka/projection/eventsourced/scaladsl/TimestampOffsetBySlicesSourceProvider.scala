/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.eventsourced.scaladsl

trait TimestampOffsetBySlicesSourceProvider {
  def minSlice: Int
  def maxSlice: Int
}
