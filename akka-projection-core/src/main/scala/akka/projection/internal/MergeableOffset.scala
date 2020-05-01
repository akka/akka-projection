package akka.projection.internal

object MergeableOffsets {
  type SurrogateProjectionKey = String
  final case class Offset[InnerOffset](entries: Map[SurrogateProjectionKey, InnerOffset])
}
