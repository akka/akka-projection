/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import java.util.Optional

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

import akka.Done
import akka.annotation.ApiMayChange
import akka.projection.ProjectionId
import com.github.ghik.silencer.silent

@ApiMayChange
object TestInMemoryOffsetStore {

  /**
   * An in-memory offset store that may be used with a [[TestProjection]].
   */
  def apply[Offset](): TestInMemoryOffsetStore[Offset] =
    new TestInMemoryOffsetStore[Offset]()

  /**
   * An in-memory offset store that may be used with a [[TestProjection]].
   */
  def create[Offset](): TestInMemoryOffsetStore[Offset] = apply()
}

@ApiMayChange
class TestInMemoryOffsetStore[Offset] private[projection] () {
  private var savedOffsets = List[(ProjectionId, Offset)]()

  /**
   * The last saved offset to the offset store.
   */
  def lastOffset(): Option[Offset] = this.synchronized(savedOffsets.headOption.map { case (_, offset) => offset })

  /**
   * Java API: The last saved offset to the offset store.
   */
  def lastOffsetJava(): Optional[Offset] = lastOffset().asJava

  /**
   * All offsets saved to the offset store.
   */
  def allOffsets(): List[(ProjectionId, Offset)] = this.synchronized(savedOffsets)

  /**
   * Java API: All offsets saved to the offset store.
   */
  @silent
  def allOffsetsJava(): java.util.List[akka.japi.Pair[ProjectionId, Offset]] =
    savedOffsets.map { case (id, offset) => akka.japi.Pair(id, offset) }.asJava

  def readOffsets(): Future[Option[Offset]] = this.synchronized { Future.successful(lastOffset()) }

  def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] = this.synchronized {
    savedOffsets = (projectionId -> offset) +: savedOffsets
    Future.successful(Done)
  }
}
