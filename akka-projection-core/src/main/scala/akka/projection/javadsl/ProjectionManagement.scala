/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.projection.ProjectionId
import akka.projection.scaladsl

@ApiMayChange object ProjectionManagement {
  def get(system: ActorSystem[_]): ProjectionManagement = new ProjectionManagement(system)
}

@ApiMayChange class ProjectionManagement(system: ActorSystem[_]) {
  private val delegate = scaladsl.ProjectionManagement(system)
  private implicit val ec: ExecutionContext = system.executionContext

  /**
   * Get the latest stored offset for the `projectionId`.
   */
  def getOffset[Offset](projectionId: ProjectionId): CompletionStage[Optional[Offset]] =
    delegate.getOffset[Offset](projectionId).map(_.asJava).toJava

  /**
   * Update the stored offset for the `projectionId` and restart the `Projection`.
   * This can be useful if the projection was stuck with errors on a specific offset and should skip
   * that offset and continue with next. Note that when the projection is restarted it will continue from
   * the next offset that is greater than the stored offset.
   */
  def updateOffset[Offset](projectionId: ProjectionId, offset: Offset): CompletionStage[Done] =
    delegate.updateOffset[Offset](projectionId, offset).toJava

  /**
   * Clear the stored offset for the `projectionId` and restart the `Projection`.
   * This can be useful if the projection should be completely rebuilt, starting over again from the first
   * offset.
   */
  def clearOffset(projectionId: ProjectionId): CompletionStage[Done] =
    delegate.clearOffset(projectionId).toJava

  /**
   * Is the given Projection paused or not?
   */
  def isProjectionPaused(projectionId: ProjectionId): CompletionStage[Boolean] = {
    delegate.isProjectionPaused(projectionId).toJava
  }

  /**
   * Pause the given Projection. Processing will be stopped.
   * While the Projection is paused other management operations can be performed, such as
   * [[ProjectionManagement.resumeProjection]].
   * The Projection can be resumed with [[ProjectionManagement.resumeProjection]].
   *
   * The paused/resumed state is stored and is read when the Projections are started, for example
   * in case of rebalance or system restart.
   */
  def pauseProjection(projectionId: ProjectionId): CompletionStage[Done] =
    delegate.pauseProjection(projectionId).toJava

  /**
   * Resume a paused Projection. Processing will be start from previously stored offset.
   *
   * The paused/resumed state is stored and is read when the Projections are started, for example
   * in case of rebalance or system restart.
   */
  def resumeProjection(projectionId: ProjectionId): CompletionStage[Done] =
    delegate.resumeProjection(projectionId).toJava

}
