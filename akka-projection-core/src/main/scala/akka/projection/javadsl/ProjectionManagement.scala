/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
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

}
