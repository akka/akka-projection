/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.internal

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.projection.ProjectionId
import akka.projection.testkit.scaladsl.TestOffsetStore
import com.github.ghik.silencer.silent

@InternalApi private[projection] class TestOffsetStoreAdapter[Offset](
    delegate: akka.projection.testkit.javadsl.TestOffsetStore[Offset])
    extends TestOffsetStore[Offset] {

  /**
   * The last saved offset to the offset store.
   */
  override def lastOffset(): Option[Offset] = delegate.lastOffset().asScala

  /**
   * All offsets saved to the offset store.
   */
  @silent override def allOffsets(): List[(ProjectionId, Offset)] = delegate.allOffsets().asScala.map(_.toScala).toList

  override def readOffsets(): Future[Option[Offset]] = {
    implicit val ec = akka.dispatch.ExecutionContexts.parasitic
    delegate.readOffsets().toScala.map(_.asScala)
  }

  override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] =
    delegate.saveOffset(projectionId, offset).toScala
}
