/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.r2dbc.internal

import akka.annotation.DoNotInherit
import akka.annotation.InternalStableApi

@InternalStableApi
object R2dbcOffsetValidationObserver {
  // effectively sealed, but to avoid translation the R2dbcOffsetStore.Validation extend this
  @DoNotInherit
  trait OffsetValidation

  object OffsetValidation {
    @DoNotInherit trait Accepted extends OffsetValidation
    @DoNotInherit trait Duplicate extends OffsetValidation
    @DoNotInherit trait Rejected extends OffsetValidation
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
