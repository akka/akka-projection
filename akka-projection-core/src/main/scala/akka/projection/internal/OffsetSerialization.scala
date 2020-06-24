/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.util.Base64
import java.util.UUID

import scala.collection.immutable

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.query
import akka.projection.MergeableKey
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.StringKey
import akka.serialization.SerializationExtension
import akka.serialization.Serializers

/**
 * INTERNAL API
 */
@InternalApi private[akka] object OffsetSerialization {
  sealed trait StorageRepresentation
  final case class SingleOffset(id: ProjectionId, manifest: String, offsetStr: String, mergeable: Boolean = false)
      extends StorageRepresentation
  final case class MultipleOffsets(reps: immutable.Seq[SingleOffset]) extends StorageRepresentation

  final val StringManifest = "STR"
  final val LongManifest = "LNG"
  final val IntManifest = "INT"
  final val SequenceManifest = "SEQ"
  final val TimeBasedUUIDManifest = "TBU"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class OffsetSerialization(system: ActorSystem[_]) {
  import OffsetSerialization._

  private val serialization = SerializationExtension(system)

  /**
   * Deserialize an offset from a storage representation of one or more offsets.
   * The offset is converted from its string representation to its real type.
   */
  def fromStorageRepresentation[Offset, Inner](rep: StorageRepresentation): Offset = {
    val offset: Offset = rep match {
      case SingleOffset(_, manifest, offsetStr, _) => fromStorageRepresentation[Offset](offsetStr, manifest)
      case MultipleOffsets(reps) =>
        val offsets: Map[StringKey, Inner] = reps.map {
          case SingleOffset(id, manifest, offsetStr, _) =>
            StringKey(id.key) -> fromStorageRepresentation[Inner](offsetStr, manifest)
        }.toMap
        MergeableOffset[StringKey, Inner](offsets).asInstanceOf[Offset]
    }
    offset
  }

  /**
   * Deserialize an offset from a stored string representation and manifest.
   * The offset is converted from its string representation to its real type.
   */
  def fromStorageRepresentation[Offset](offsetStr: String, manifest: String): Offset =
    (manifest match {
      case StringManifest        => offsetStr
      case LongManifest          => offsetStr.toLong
      case IntManifest           => offsetStr.toInt
      case SequenceManifest      => query.Offset.sequence(offsetStr.toLong)
      case TimeBasedUUIDManifest => query.Offset.timeBasedUUID(UUID.fromString(offsetStr))
      case _ =>
        val parts = manifest.split(':')
        val serializerId = parts(0).toInt
        val serializerManifest = parts(1)
        val bytes = Base64.getDecoder.decode(offsetStr)
        serialization.deserialize(bytes, serializerId, serializerManifest).get
    }).asInstanceOf[Offset]

  /**
   * Convert the offset to a tuple (String, String) where the first element is
   * the String representation of the offset and the second its manifest
   */
  def toStorageRepresentation[Offset](
      id: ProjectionId,
      offset: Offset,
      mergeable: Boolean = false): StorageRepresentation = {
    val reps = offset match {
      case s: String                => SingleOffset(id, StringManifest, s, mergeable)
      case l: Long                  => SingleOffset(id, LongManifest, l.toString, mergeable)
      case i: Int                   => SingleOffset(id, IntManifest, i.toString, mergeable)
      case seq: query.Sequence      => SingleOffset(id, SequenceManifest, seq.value.toString, mergeable)
      case tbu: query.TimeBasedUUID => SingleOffset(id, TimeBasedUUIDManifest, tbu.value.toString, mergeable)
      case mrg: MergeableOffset[_, _] =>
        val list = mrg.entries.map {
          case (key: MergeableKey, innerOffset) =>
            toStorageRepresentation(ProjectionId(id.name, key.surrogateKey), innerOffset, mergeable = true)
              .asInstanceOf[SingleOffset]
        }.toList

        MultipleOffsets(list)
      case _ =>
        val obj = offset.asInstanceOf[AnyRef]
        val serializer = serialization.findSerializerFor(obj)
        val serializerId = serializer.identifier
        val serializerManifest = Serializers.manifestFor(serializer, obj)
        val bytes = serializer.toBinary(obj)
        val offsetStr = Base64.getEncoder.encodeToString(bytes)
        SingleOffset(id, s"$serializerId:$serializerManifest", offsetStr)
    }
    reps
  }
}
