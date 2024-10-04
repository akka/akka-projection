/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._
import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.pubsub.Topic
import akka.actor.typed.scaladsl.AskPattern._
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.util.Timeout

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

object ProjectionManagement extends ExtensionId[ProjectionManagement] {
  def createExtension(system: ActorSystem[_]): ProjectionManagement = new ProjectionManagement(system)

  def get(system: ActorSystem[_]): ProjectionManagement = apply(system)
}

class ProjectionManagement(system: ActorSystem[_]) extends Extension {
  private implicit val sys: ActorSystem[_] = system
  private implicit val askTimeout: Timeout = {
    system.settings.config.getDuration("akka.projection.management.ask-timeout").toScala
  }
  private val operationTimeout: FiniteDuration =
    system.settings.config.getDuration("akka.projection.management.operation-timeout").toScala
  private val retryAttempts: Int = math.max(1, (operationTimeout / askTimeout.duration).toInt)
  private implicit val ec: ExecutionContext = system.executionContext

  import ProjectionBehavior.Internal._

  private val topics =
    new ConcurrentHashMap[String, ActorRef[Topic.Command[ProjectionManagementCommand]]]()

  private def topicName(projectionName: String): String =
    "projection-" + projectionName

  private def topic(projectionName: String): ActorRef[Topic.Command[ProjectionManagementCommand]] = {
    topics.computeIfAbsent(projectionName, _ => {
      val name = topicName(projectionName)
      system.systemActorOf(Topic[ProjectionManagementCommand](name), sanitizeActorName(name))
    })
  }

  /**
   * ProjectionBehavior registers when started
   */
  private[projection] def register(
      projectionId: ProjectionId,
      projection: ActorRef[ProjectionManagementCommand]): Unit = {
    topic(projectionId.name) ! Topic.Subscribe(projection)
  }

  /**
   * Get the latest stored offset for the `projectionId`.
   */
  def getOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] = {
    def askGetOffset(): Future[Option[Offset]] =
      topic(projectionId.name)
        .ask[CurrentOffset[Offset]](replyTo => Topic.Publish(GetOffset(projectionId, replyTo)))
        .map(currentOffset => currentOffset.offset)
    retry(() => askGetOffset())
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
    def askSetOffset(): Future[Done] = {
      topic(projectionId.name)
        .ask(replyTo => Topic.Publish(SetOffset(projectionId, offset, replyTo)))
    }
    retry(() => askSetOffset())
  }

  private def retry[T](operation: () => Future[T]): Future[T] = {
    def attempt(remaining: Int): Future[T] = {
      operation().recoverWith {
        case e: TimeoutException =>
          if (remaining > 0) attempt(remaining - 1)
          else Future.failed(e)
      }
    }

    attempt(retryAttempts)
  }

  /**
   * Is the given Projection paused or not?
   */
  def isPaused(projectionId: ProjectionId): Future[Boolean] = {
    def askIsPaused(): Future[Boolean] = {
      topic(projectionId.name)
        .ask(replyTo => Topic.Publish(IsPaused(projectionId, replyTo)))
    }

    retry(() => askIsPaused())
  }

  /**
   * Pause the given Projection. Processing will be stopped.
   * While the Projection is paused other management operations can be performed, such as
   * [[ProjectionManagement.updateOffset]].
   * The Projection can be resumed with [[ProjectionManagement.resume]].
   *
   * The paused/resumed state is stored and, and it is read when the Projections are started, for example
   * in case of rebalance or system restart.
   */
  def pause(projectionId: ProjectionId): Future[Done] =
    setPauseProjection(projectionId, paused = true)

  /**
   * Resume a paused Projection. Processing will be start from previously stored offset.
   *
   * The paused/resumed state is stored and, and it is read when the Projections are started, for example
   * in case of rebalance or system restart.
   */
  def resume(projectionId: ProjectionId): Future[Done] =
    setPauseProjection(projectionId, paused = false)

  private def setPauseProjection(projectionId: ProjectionId, paused: Boolean): Future[Done] = {
    def askSetPaused(): Future[Done] = {
      topic(projectionId.name)
        .ask(replyTo => Topic.Publish(SetPaused(projectionId, paused, replyTo)))
    }
    retry(() => askSetPaused())
  }

  private def sanitizeActorName(text: String): String =
    URLEncoder.encode(text, StandardCharsets.UTF_8.name())
}
