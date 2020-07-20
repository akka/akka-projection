/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.util.control.NoStackTrace

import akka.annotation.ApiMayChange
import akka.annotation.InternalApi

sealed trait OffsetVerification

@ApiMayChange
object OffsetVerification {
  case object VerificationSuccess extends OffsetVerification

  /** Java API */
  def verificationSuccess: OffsetVerification = VerificationSuccess

  final case class VerificationFailure[Offset](reason: String) extends OffsetVerification

  /** Java API */
  def verificationFailure(reason: String): OffsetVerification = VerificationFailure(reason)

  /**
   * Internal API
   *
   * Used when verifying offsets as part of transaction.
   */
  @InternalApi private[projection] case object VerificationFailureException
      extends RuntimeException("Offset verification failed")
      with NoStackTrace
}
