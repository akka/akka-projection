package akka.projection.internal

object MergeableOffsets {
  def empty: Offset = Offset(Set.empty)
  def one(row: OffsetRow): Offset = Offset(Set(row))

  object OffsetRow {
    // TODO: use proper serialization
    def fromString(str: String): OffsetRow = {
      str.split(",").toSeq match {
        case name :: offset :: Nil if offset.toLongOption.isDefined => OffsetRow(name, offset.toLong)
      }
    }
  }

  final case class OffsetRow(name: String, offset: Long) {
    override def toString: String = s"$name,$offset"
  }

  final case class Offset(entries: Set[OffsetRow]) {
    // TODO: find a more efficient algorithm
    def merge(other: Offset): Offset = {
      val maxEntries = entries
        .union(other.entries)
        .groupBy(_.name)
        .map {
          case (name, entries) => OffsetRow(name, entries.maxBy(_.offset).offset)
        }
        .toSet
      Offset(maxEntries)
    }
  }
}
