/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.pubsub.Topic
import akka.actor.typed.scaladsl.AskPattern._
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.util.Timeout

@ApiMayChange object ProjectionManagement extends ExtensionId[ProjectionManagement] {
  def createExtension(system: ActorSystem[_]): ProjectionManagement = new ProjectionManagement(system)

  def get(system: ActorSystem[_]): ProjectionManagement = apply(system)
}

@ApiMayChange class ProjectionManagement(system: ActorSystem[_]) extends Extension {
  private implicit val sys: ActorSystem[_] = system
  private implicit val askTimeout: Timeout = {
    import akka.util.JavaDurationConverters._
    system.settings.config.getDuration("akka.projection.management.operation-timeout").asScala
  }
  private implicit val ec: ExecutionContext = system.executionContext

  import ProjectionBehavior.Internal._

  private val topics =
    new ConcurrentHashMap[String, ActorRef[Topic.Command[OffsetManagementCommand]]]()

  private def topicName(projectionName: String): String =
    "projection-" + projectionName

  private def topic(projectionName: String): ActorRef[Topic.Command[OffsetManagementCommand]] = {
    topics.computeIfAbsent(projectionName, _ => {
      val name = topicName(projectionName)
      system.systemActorOf(Topic[OffsetManagementCommand](name), name)
    })
  }

  private[projection] def register(projectionId: ProjectionId, projection: ActorRef[OffsetManagementCommand]): Unit = {
    topic(projectionId.name) ! Topic.Subscribe(projection)
  }

  /**
   * Get the latest stored offset for the `projectionId`.
   */
  def getOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] = {
    topic(projectionId.name)
      .ask[CurrentOffset[Offset]](replyTo => Topic.Publish(GetOffset(projectionId, replyTo)))
      .map(currentOffset => currentOffset.offset)
  }

  /**
   * Update the stored offset for the `projectionId` and restart the `Projection`.
   * This can be useful if the projection was stuck with errors on a specific offset and should skip
   * that offset and continue with next. Note that when the projection is restarted it will continue from
   * the next offset that is greater than the stored offset.
   */
  def updateOffset[Offset](projectionId: ProjectionId, offset: Offset): Future[Done] =
    setOffset(projectionId, Some(offset))

  /**
   * Clear the stored offset for the `projectionId` and restart the `Projection`.
   * This can be useful if the projection should be completely rebuilt, starting over again from the first
   * offset.
   */
  def clearOffset(projectionId: ProjectionId): Future[Done] =
    setOffset(projectionId, None)

  private def setOffset[Offset](projectionId: ProjectionId, offset: Option[Offset]): Future[Done] = {
    topic(projectionId.name)
      .ask(replyTo => Topic.Publish(SetOffset(projectionId, offset, replyTo)))
  }
}
