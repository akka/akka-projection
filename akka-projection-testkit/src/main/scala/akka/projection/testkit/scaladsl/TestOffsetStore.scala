/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.testkit.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.annotation.DoNotInherit
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState

@DoNotInherit
trait TestOffsetStore[Offset] {

  /**
   * The last saved offset to the offset store.
   */
  def lastOffset(): Option[Offset]

  /**
   * All offsets saved to the offset store.
   */
  def allOffsets(): List[(ProjectionId, Offset)]

  def readOffsets(): Future[Option[Offset]]

  def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done]

  def readManagementState(projectionId: ProjectionId): Future[Option[ManagementState]]

  def savePaused(projectionId: ProjectionId, paused: Boolean): Future[Done]
}
