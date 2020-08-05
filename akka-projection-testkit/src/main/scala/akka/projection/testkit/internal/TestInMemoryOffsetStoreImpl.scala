/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.internal

import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.projection.ProjectionId
import akka.projection.testkit.scaladsl.TestOffsetStore

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class TestInMemoryOffsetStoreImpl[Offset] private[projection] () extends TestOffsetStore[Offset] {
  private var savedOffsets = List[(ProjectionId, Offset)]()

  /**
   * The last saved offset to the offset store.
   */
  def lastOffset(): Option[Offset] = this.synchronized(savedOffsets.headOption.map { case (_, offset) => offset })

  /**
   * All offsets saved to the offset store.
   */
  def allOffsets(): List[(ProjectionId, Offset)] = this.synchronized(savedOffsets)

  def readOffsets(): Future[Option[Offset]] = this.synchronized { Future.successful(lastOffset()) }

  def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] = this.synchronized {
    savedOffsets = (projectionId -> offset) +: savedOffsets
    Future.successful(Done)
  }
}
