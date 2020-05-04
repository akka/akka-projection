/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

object MergeableOffsets {
  type SurrogateProjectionKey = String
  final case class Offset[InnerOffset](entries: Map[SurrogateProjectionKey, InnerOffset])
}
