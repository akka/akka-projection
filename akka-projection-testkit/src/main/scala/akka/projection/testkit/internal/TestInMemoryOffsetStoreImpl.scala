/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.internal

import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.testkit.scaladsl.TestOffsetStore

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class TestInMemoryOffsetStoreImpl[Offset] private[projection] () extends TestOffsetStore[Offset] {
  private var savedOffsets = List[(ProjectionId, Offset)]()
  private var savedPaused = Map.empty[ProjectionId, Boolean]

  override def lastOffset(): Option[Offset] =
    this.synchronized(savedOffsets.headOption.map { case (_, offset) => offset })

  override def allOffsets(): List[(ProjectionId, Offset)] = this.synchronized(savedOffsets)

  override def readOffsets(): Future[Option[Offset]] = this.synchronized { Future.successful(lastOffset()) }

  override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] = this.synchronized {
    savedOffsets = (projectionId -> offset) +: savedOffsets
    Future.successful(Done)
  }

  override def readManagementState(projectionId: ProjectionId): Future[Option[ManagementState]] = this.synchronized {
    Future.successful(savedPaused.get(projectionId).map(ManagementState.apply))
  }

  override def savePaused(projectionId: ProjectionId, paused: Boolean): Future[Done] = this.synchronized {
    savedPaused = savedPaused.updated(projectionId, paused)
    Future.successful(Done)
  }
}
