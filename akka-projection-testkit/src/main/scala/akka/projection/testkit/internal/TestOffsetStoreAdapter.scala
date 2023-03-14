/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.internal

import akka.util.ccompat.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.testkit.scaladsl.TestOffsetStore
@InternalApi private[projection] class TestOffsetStoreAdapter[Offset](
    delegate: akka.projection.testkit.javadsl.TestOffsetStore[Offset])
    extends TestOffsetStore[Offset] {

  override def lastOffset(): Option[Offset] = delegate.lastOffset().asScala

  override def allOffsets(): List[(ProjectionId, Offset)] = delegate.allOffsets().asScala.map(_.toScala).toList

  override def readOffsets(): Future[Option[Offset]] = {
    implicit val ec = akka.dispatch.ExecutionContexts.parasitic
    delegate.readOffsets().toScala.map(_.asScala)
  }

  override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] =
    delegate.saveOffset(projectionId, offset).toScala

  override def readManagementState(projectionId: ProjectionId): Future[Option[ManagementState]] = {
    implicit val ec = akka.dispatch.ExecutionContexts.parasitic
    delegate.readManagementState(projectionId).toScala.map(_.asScala)
  }

  override def savePaused(projectionId: ProjectionId, paused: Boolean): Future[Done] =
    delegate.savePaused(projectionId, paused).toScala
}
