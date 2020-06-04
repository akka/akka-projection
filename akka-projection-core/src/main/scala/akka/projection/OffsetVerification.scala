/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

sealed trait OffsetVerification

object OffsetVerification {
  case object VerificationSuccess extends OffsetVerification

  /** Java API */
  def verificationSuccess: OffsetVerification = VerificationSuccess

  final case class VerificationFailure[Offset](reason: String) extends OffsetVerification

  /** Java API */
  def verificationFailure(reason: String): OffsetVerification = VerificationFailure(reason)
}
