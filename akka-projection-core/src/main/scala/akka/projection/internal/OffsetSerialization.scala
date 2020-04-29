/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.util.UUID

import akka.annotation.InternalApi
import akka.persistence.query

/**
 * INTERNAL API
 */
@InternalApi private[akka] object OffsetSerialization {
  final val StringManifest = "STR"
  final val LongManifest = "LNG"
  final val IntManifest = "INT"
  final val SequenceManifest = "SEQ"
  final val TimeBasedUUIDManifest = "TBU"
  final val MergeableManifest = "MRG"

  /**
   * Deserialize an offset from a stored string representation and manifest.
   * The offset is converted from its string representation to its real type.
   */
  def fromStorageRepresentation[Offset](offsetRows: Seq[(String, String)]): Offset = {
    require(offsetRows.nonEmpty, "At least one storage representation is required")
    val offsets = offsetRows.map {
      case (offsetStr, manifest) => fromStorageRepresentation(offsetStr, manifest)
    }

    if (offsets.forall(_.isInstanceOf[MergeableOffsets.Offset]))
      offsets
        .map(_.asInstanceOf[MergeableOffsets.Offset])
        .fold(MergeableOffsets.empty)(_.merge(_))
        .asInstanceOf[Offset]
    else offsets.head
  }

  def fromStorageRepresentation[Offset](offsetStr: String, manifest: String): Offset =
    (manifest match {
      case StringManifest        => offsetStr
      case LongManifest          => offsetStr.toLong
      case IntManifest           => offsetStr.toInt
      case SequenceManifest      => query.Offset.sequence(offsetStr.toLong)
      case TimeBasedUUIDManifest => query.Offset.timeBasedUUID(UUID.fromString(offsetStr))
      case MergeableManifest     => MergeableOffsets.OffsetRow.fromString(offsetStr)
    }).asInstanceOf[Offset]

  /**
   * Convert the offset to a tuple (String, String) where the first element is
   * the String representation of the offset and the second its manifest
   */
  def toStorageRepresentation[Offset](offset: Offset): Seq[(String, String)] = {
    val reps = offset match {
      case s: String                    => List(s -> StringManifest)
      case l: Long                      => List(l.toString -> LongManifest)
      case i: Int                       => List(i.toString -> IntManifest)
      case seq: query.Sequence          => List(seq.value.toString -> SequenceManifest)
      case tbu: query.TimeBasedUUID     => List(tbu.value.toString -> TimeBasedUUIDManifest)
      case mrg: MergeableOffsets.Offset => mrg.entries.map(_.toString -> MergeableManifest).toSeq
      case _                            => throw new IllegalArgumentException(s"Unsupported offset type, found [${offset.getClass.getName}]")
    }
    require(reps.nonEmpty, "The Offset must produce at least one storage representation entry")
    reps
  }
}
