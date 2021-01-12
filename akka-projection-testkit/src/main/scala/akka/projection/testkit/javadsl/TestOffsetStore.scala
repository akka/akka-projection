/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.Done
import akka.projection.ProjectionId

trait TestOffsetStore[Offset] {

  /**
   * The last saved offset to the offset store.
   */
  def lastOffset(): Optional[Offset]

  /**
   * All offsets saved to the offset store.
   */
  def allOffsets(): java.util.List[akka.japi.Pair[ProjectionId, Offset]]

  def readOffsets(): CompletionStage[Optional[Offset]]

  def saveOffset(projectionId: ProjectionId, offset: Offset): CompletionStage[Done]
}
