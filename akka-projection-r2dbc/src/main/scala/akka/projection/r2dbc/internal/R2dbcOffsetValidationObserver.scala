/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.r2dbc.internal

import akka.annotation.InternalStableApi

@InternalStableApi
object R2dbcOffsetValidationObserver {
  sealed trait OffsetValidation

  object OffsetValidation {
    case object Accepted extends OffsetValidation
    case object Duplicate extends OffsetValidation
    case object RejectedSeqNr extends OffsetValidation
    case object RejectedBacktrackingSeqNr extends OffsetValidation
  }
}

/**
 * [[akka.projection.internal.Telemetry]] may implement this trait for offset validation progress tracking.
 */
@InternalStableApi
trait R2dbcOffsetValidationObserver {
  import R2dbcOffsetValidationObserver._

  def onOffsetValidated[Envelope](envelope: Envelope, result: OffsetValidation): Unit

}
