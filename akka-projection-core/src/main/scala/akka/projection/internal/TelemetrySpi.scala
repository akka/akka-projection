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
  def beforeProcess[Offset, Envelope](projectionId: ProjectionId): AnyRef = { ??? }
  def afterProcess[Offset, Envelope](projectionId: ProjectionId, telemetryContext: AnyRef): Unit = {}
  // Only invoked when the offset is committed. Pass the context of all the envelopes committed
  def onEnvelopeSuccess[Offset, Envelope](projectionId: ProjectionId, telemetryContexts: AnyRef*): Unit = {}
  // Invoked when processing an envelope fails. If the operation is part of a batch
  // or a group it will be invoked once anyway
  def error[Offset, Envelope](projectionId: ProjectionId, cause: Throwable): Unit = {}
}
