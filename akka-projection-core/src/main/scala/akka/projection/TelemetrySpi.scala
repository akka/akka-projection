/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.collection.immutable

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi

/**
 * Service Provider Interface (SPI) for collecting metrics from projections.
 *
 * Implementations must include a single constructor with two arguments: [[ProjectionId]]
 * and [[ActorSystem[_]].  To setup your implementation, add a setting on your `application.conf`:
 *
 * {{{
 * akka.projection.telemetry.fqcn = com.example.MyMetrics
 * }}}
 */
trait Telemetry {

  /** Invoked when a projection is stopped gracefully. */
  def stopped(): Unit

  /**
   * Invoked when a projection is stopped unexpectedly.
   *
   * @param cause exception thrown by the errored envelope handler.
   */
  def failed(cause: Throwable): Unit

  /**
   * Invoked after processing an event such that it is visible by the read-side threads (data is
   * committed).  This method is granted to be invoked after the envelope handler has committed but
   * may or may not happen after the offset was committed (depending on the projection semantics).
   *
   * @param readyTimestampNanos timestamp indicating when this envelope arrived at the projection.  That
   *                            timestamp happens in the projection code so it happens outside the source
   *                            provider (e.g. doesn't include deserializing the envelope from the wire).
   */
  def afterProcess(readyTimestampNanos: Long): Unit

  /**
   * Invoked when the offset is committed.
   *
   * @param numberOfEnvelopes number of envelopes marked as committed when committing this offset.  This takes
   *                  into consideration both batched processing (only commit one offset every N
   *                  envelopes) and grouped handling (user code processes multiple envelopes at
   *                  once).
   */
  def onOffsetStored(numberOfEnvelopes: Int): Unit

  /**
   * Invoked when processing an envelope errors.  When using a [[HandlerRecoveryStrategy]] that
   * retries, this method will be invoked as many times as retries.
   *
   * @param cause exception thrown by the errored envelope handler.
   */
  def error(cause: Throwable): Unit
}

/**
 * INTERNAL API
 */
@InternalStableApi private[akka] object TelemetryProvider {
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
@InternalApi private[akka] object NoopTelemetry extends Telemetry {
  override def failed(cause: Throwable): Unit = {}

  override def stopped(): Unit = {}

  override def afterProcess(readyTimestampNanos: Long): Unit = {}

  override def onOffsetStored(numberOfEnvelopes: Int): Unit = {}

  override def error(cause: Throwable): Unit = {}

}
