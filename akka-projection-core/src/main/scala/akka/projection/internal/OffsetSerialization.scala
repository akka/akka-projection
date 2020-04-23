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

  /**
   * Deserialize an offset from a stored string representation and manifest.
   * The offset is converted from its string representation to its real type.
   */
  def fromStorageRepresentation[Offset](offsetStr: String, manifest: String): Offset = {
    (manifest match {
      case StringManifest        => offsetStr
      case LongManifest          => offsetStr.toLong
      case IntManifest           => offsetStr.toInt
      case SequenceManifest      => query.Offset.sequence(offsetStr.toLong)
      case TimeBasedUUIDManifest => query.Offset.timeBasedUUID(UUID.fromString(offsetStr))
    }).asInstanceOf[Offset]
  }

  /**
   * Convert the offset to a tuple (String, String) where the first element is
   * the String representation of the offset and the second its manifest
   */
  def toStorageRepresentation[Offset](offset: Offset): (String, String) = {
    offset match {
      case s: String                => s -> StringManifest
      case l: Long                  => l.toString -> LongManifest
      case i: Int                   => i.toString -> IntManifest
      case seq: query.Sequence      => seq.value.toString -> SequenceManifest
      case tbu: query.TimeBasedUUID => tbu.value.toString -> TimeBasedUUIDManifest
      case _                        => throw new IllegalArgumentException(s"Unsupported offset type, found [${offset.getClass.getName}]")
    }
  }
}
