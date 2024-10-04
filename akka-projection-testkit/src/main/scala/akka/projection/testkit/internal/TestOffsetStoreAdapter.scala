/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.internal

import scala.jdk.OptionConverters._
import scala.jdk.FutureConverters._
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import akka.Done
import akka.annotation.InternalApi
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.testkit.scaladsl.TestOffsetStore

@InternalApi private[projection] class TestOffsetStoreAdapter[Offset](
    delegate: akka.projection.testkit.javadsl.TestOffsetStore[Offset])
    extends TestOffsetStore[Offset] {

  override def lastOffset(): Option[Offset] = delegate.lastOffset().toScala

  override def allOffsets(): List[(ProjectionId, Offset)] = delegate.allOffsets().asScala.map(_.toScala).toList

  override def readOffsets(): Future[Option[Offset]] = {
    implicit val ec = scala.concurrent.ExecutionContext.parasitic
    delegate.readOffsets().asScala.map(_.toScala)
  }

  override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] =
    delegate.saveOffset(projectionId, offset).asScala

  override def readManagementState(projectionId: ProjectionId): Future[Option[ManagementState]] = {
    implicit val ec = scala.concurrent.ExecutionContext.parasitic
    delegate.readManagementState(projectionId).asScala.map(_.toScala)
  }

  override def savePaused(projectionId: ProjectionId, paused: Boolean): Future[Done] =
    delegate.savePaused(projectionId, paused).asScala
}
