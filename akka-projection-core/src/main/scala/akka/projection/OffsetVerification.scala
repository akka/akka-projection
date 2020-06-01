package akka.projection

sealed trait OffsetVerification

object OffsetVerification {
  case object VerificationSuccess extends OffsetVerification
  final case class VerificationFailure[Offset](reason: String) extends OffsetVerification
}
