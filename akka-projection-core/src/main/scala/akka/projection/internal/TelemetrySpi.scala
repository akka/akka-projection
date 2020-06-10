/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.projection.ProjectionId

object TelemetryProvider {
  // this is separate because it creates a bunch of internals
  // provided in the Telemetry instance
  def started(projectionId: ProjectionId): Telemetry = {
    ???
  }
}
class Telemetry {

  // Per projection
  def failed(projectionId: ProjectionId, cause: Throwable): Unit = { /* will release all internals*/ }
  def stopped(projectionId: ProjectionId): Unit = { /* will release all internals*/ }

  // Per envelope
  def onEnvelopeReady[Offset, Envelope](projectionId: ProjectionId, offset: Offset, env: Envelope): AnyRef = { ??? }
  def onEnvelopeSuccess[Offset, Envelope](projectionId: ProjectionId, offset: Offset, env: Envelope): Unit = {}
  def error[Offset, Envelope](projectionId: ProjectionId, offset: Offset, env: Envelope, cause: Throwable): Unit = {}
}
