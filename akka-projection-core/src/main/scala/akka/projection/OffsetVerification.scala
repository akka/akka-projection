package akka.projection

sealed trait OffsetVerification

case object Success extends OffsetVerification
final case class SkipOffset[Offset](reason: String) extends OffsetVerification
