/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.projection.ProjectionId

object TelemetryProvider {
  def start(projectionId: ProjectionId, system: ActorSystem[_]): Telemetry = {
    val dynamicAccess = system.dynamicAccess
    if (system.settings.config.hasPath("akka.projection.telemetry.fqcn")) {
      val telemetryFqcn = system.settings.config.getString("akka.projection.telemetry.fqcn")
      dynamicAccess
        .createInstanceFor[Telemetry](
          telemetryFqcn,
          Seq((classOf[ProjectionId], projectionId), (classOf[ActorSystem[_]], system)))
        .get
    } else {
      // TODO: review this default: should use a default value in reference.conf instead?
      NoopTelemetry
    }
  }
}

abstract class Telemetry(projectionId: ProjectionId, system: ActorSystem[_]) {

  // Per projection
  def failed(projectionId: ProjectionId, cause: Throwable): Unit
  def stopped(projectionId: ProjectionId): Unit

  // Per envelope
  def beforeProcess(projectionId: ProjectionId): AnyRef
  def afterProcess(projectionId: ProjectionId, telemetryContext: AnyRef): Unit

  /** Only invoked when the offset is committed. Pass the number of envelopes committed */
  def onOffsetStored(projectionId: ProjectionId, batchSize: Int): Unit
  // Invoked when processing an envelope fails. If the operation is part of a batch
  // or a group it will be invoked once anyway
  def error(projectionId: ProjectionId, cause: Throwable): Unit
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object NoopTelemetry extends Telemetry(null, null) {
  override def failed(projectionId: ProjectionId, cause: Throwable): Unit = {}

  override def stopped(projectionId: ProjectionId): Unit = {}

  override def beforeProcess(projectionId: ProjectionId): AnyRef = null

  override def afterProcess(projectionId: ProjectionId, telemetryContext: AnyRef): Unit = {}

  /** Only invoked when the offset is committed. Pass the number of envelopes committed */
  override def onOffsetStored(projectionId: ProjectionId, batchSize: Int): Unit = {}

  override def error(projectionId: ProjectionId, cause: Throwable): Unit = {}
}
