/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorSystem
import akka.projection.ProjectionId

object TelemetryProvider {
  def start(projectionId: ProjectionId)(system: ActorSystem[_]): Telemetry = {
    val dynamicAccess = system.dynamicAccess
    val telemetryFqcn = system.settings.config.getString("akka.projection.telemetry.fqcn")
    val t = dynamicAccess.createInstanceFor[Telemetry](telemetryFqcn, Nil).get
    t.started(projectionId)
    t
  }
}

abstract class Telemetry {

  // Per projection
  private[projection] def started(projectionId: ProjectionId): Unit
  def failed(projectionId: ProjectionId, cause: Throwable): Unit
  def stopped(projectionId: ProjectionId): Unit

  // Per envelope
  def beforeProcess[Offset, Envelope](projectionId: ProjectionId): AnyRef
  def afterProcess[Offset, Envelope](projectionId: ProjectionId, telemetryContext: AnyRef): Unit
  // Only invoked when the offset is committed. Pass the number of envelopes committed
  def onEnvelopeSuccess[Offset, Envelope](projectionId: ProjectionId, successCount: Int): Unit
  // Invoked when processing an envelope fails. If the operation is part of a batch
  // or a group it will be invoked once anyway
  def error[Offset, Envelope](projectionId: ProjectionId, cause: Throwable): Unit
}
