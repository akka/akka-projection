/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.collection.immutable

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.projection.ProjectionId

/**
 * INTERNAL API
 */
@InternalStableApi private[projection] object TelemetryProvider {
  def start(projectionId: ProjectionId, system: ActorSystem[_]): Telemetry = {
    val dynamicAccess = system.dynamicAccess
    if (system.settings.config.hasPath("akka.projection.telemetry.fqcn")) {
      val telemetryFqcn: String = system.settings.config.getString("akka.projection.telemetry.fqcn")
      dynamicAccess
        .createInstanceFor[Telemetry](
          telemetryFqcn,
          immutable.Seq((classOf[ProjectionId], projectionId), (classOf[ActorSystem[_]], system)))
        .get
    } else {
      // TODO: review this default: should use a default value in reference.conf instead?
      NoopTelemetry
    }
  }
}

/**
 * INTERNAL API
 */
@InternalStableApi private[akka] trait Telemetry {

  // Per projection
  def failed(cause: Throwable): Unit
  def stopped(): Unit

  // Per envelope
  /** Invoked after processing an event such that it is visible by the read-side threads (data must be committed).*/
  private[projection] def afterProcess(serviceTimeInNanos: => Long): Unit

  /** Only invoked when the offset is committed. Pass the number of envelopes committed */
  def onOffsetStored(batchSize: Int): Unit

  /**
   * Invoked when processing an envelope errors.  If the operation is part of a batch or
   * a group it will be invoked once.
   */
  def error(cause: Throwable): Unit
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object NoopTelemetry extends Telemetry {
  override def failed(cause: Throwable): Unit = {}

  override def stopped(): Unit = {}

  override private[projection] def afterProcess(serviceTimeInNanos: => Long): Unit = {}

  override def onOffsetStored(batchSize: Int): Unit = {}

  override def error(cause: Throwable): Unit = {}

}
