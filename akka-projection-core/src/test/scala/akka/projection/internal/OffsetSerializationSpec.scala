/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.UUID

import scala.collection.immutable

import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.serialization.SerializerWithStringManifest
import akka.util.unused
import org.scalatest.wordspec.AnyWordSpecLike

object OffsetSerializationSpec {
  class TestSerializer(@unused system: ExtendedActorSystem) extends SerializerWithStringManifest {
    def identifier: Int = 9999

    def manifest(o: AnyRef): String =
      "a"

    def toBinary(o: AnyRef): Array[Byte] = o match {
      case OtherOffset(s) => s.getBytes(StandardCharsets.UTF_8)
      case _ =>
        throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
    }

    def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
      case "a" => OtherOffset(new String(bytes, StandardCharsets.UTF_8))
      case _   => throw new IllegalArgumentException(s"Unknown manifest [$manifest]")
    }
  }

  final case class OtherOffset(s: String)
}

class OffsetSerializationSpec
    extends ScalaTestWithActorTestKit("""
      akka.actor {
        serializers {
          test = "akka.projection.internal.OffsetSerializationSpec$TestSerializer"
        }
        serialization-bindings {
          "akka.projection.internal.OffsetSerializationSpec$OtherOffset" = test
        }
      }
    """)
    with AnyWordSpecLike
    with LogCapturing {
  import OffsetSerializationSpec._
  import OffsetSerialization._
  private val offsetSerialization = new OffsetSerialization(system)
  import offsetSerialization.fromStorageRepresentation
  import offsetSerialization.toStorageRepresentation

  private val id = ProjectionId("user-view", "1")
  private val longValue = Long.MaxValue - 17

  "OffsetSerialization" must {
    "convert offsets of type Long" in {
      toStorageRepresentation(id, longValue) shouldBe SingleOffset(id, LongManifest, longValue.toString)
      fromStorageRepresentation[Long](longValue.toString, LongManifest) shouldBe longValue
    }

    "convert offsets of type java.lang.Long" in {
      toStorageRepresentation(
        id,
        java.lang.Long
          .valueOf(longValue)) shouldBe SingleOffset(id, LongManifest, longValue.toString)
      fromStorageRepresentation[java.lang.Long](longValue.toString, LongManifest) shouldBe
      java.lang.Long
        .valueOf(longValue)
    }

    "convert offsets of type Int" in {
      toStorageRepresentation(id, 17) shouldBe SingleOffset(id, IntManifest, "17")
      fromStorageRepresentation[Int]("17", IntManifest) shouldBe 17
    }

    "convert offsets of type java.lang.Integer" in {
      toStorageRepresentation(id, java.lang.Integer.valueOf(17)) shouldBe SingleOffset(id, IntManifest, "17")
      fromStorageRepresentation[java.lang.Integer]("17", IntManifest) shouldBe java.lang.Integer.valueOf(17)
    }

    "convert offsets of type String" in {
      toStorageRepresentation(id, "abc") shouldBe SingleOffset(id, StringManifest, "abc")
      fromStorageRepresentation[String]("abc", StringManifest) shouldBe "abc"
    }

    "convert offsets of type akka.persistence.query.Sequence" in {
      toStorageRepresentation(id, query.Sequence(1L)) shouldBe SingleOffset(id, SequenceManifest, "1")
      fromStorageRepresentation[query.Sequence]("1", SequenceManifest) shouldBe query.Sequence(1L)
    }

    "convert offsets of type akka.persistence.query.TimeBasedUUID" in {
      //2019-12-16T15:32:36.148Z[UTC]
      val uuidString = "49225740-2019-11ea-a752-ffae2393b6e4"
      val timeOffset = query.TimeBasedUUID(UUID.fromString(uuidString))
      toStorageRepresentation(id, timeOffset) shouldBe SingleOffset(id, TimeBasedUUIDManifest, uuidString)
      fromStorageRepresentation[query.TimeBasedUUID](uuidString, TimeBasedUUIDManifest) shouldBe timeOffset
    }

    "convert other offsets types with Akka Serialization" in {
      val offsetValue = "123-åäö"
      val base64EncodedValue = Base64.getEncoder.encodeToString(offsetValue.getBytes(StandardCharsets.UTF_8))
      toStorageRepresentation(id, OtherOffset(offsetValue)) shouldBe SingleOffset(id, "9999:a", base64EncodedValue)
      fromStorageRepresentation[OtherOffset](base64EncodedValue, "9999:a") shouldBe OtherOffset(offsetValue)
    }
  }

  "OffsetSerialization of MergeableOffsets" must {
    "convert offsets of type MergeableOffset" in {
      val surrogateProjectionKey = "user-group-topic-1"
      val mergeableOffset = new MergeableOffset(Map(surrogateProjectionKey -> 1L))
      val actualRep = toStorageRepresentation(id, mergeableOffset)

      withClue("return a storage representation of MultipleOffsets") {
        actualRep.getClass shouldBe classOf[MultipleOffsets]
      }

      withClue("override provided projection key with surrogate projection key") {
        actualRep.asInstanceOf[MultipleOffsets].reps.head.id.key shouldBe surrogateProjectionKey
      }

      val storageRepresentation = MultipleOffsets(
        immutable.Seq(SingleOffset(ProjectionId(id.name, surrogateProjectionKey), LongManifest, "1", mergeable = true)))

      actualRep shouldBe storageRepresentation

      fromStorageRepresentation[MergeableOffset[Long], Long](storageRepresentation) shouldBe mergeableOffset
    }

    "merge rows into one MergeableOffset" in {
      val projectionName = "user-projection"
      val surrogateProjectionKey1 = "user-group-topic-1"
      val surrogateProjectionKey2 = "user-group-topic-2"

      val mergeableOffset =
        MergeableOffset(Map(surrogateProjectionKey1 -> 1L, surrogateProjectionKey2 -> 2L))
      val storageRepresentation = MultipleOffsets(
        immutable.Seq(
          SingleOffset(ProjectionId(projectionName, surrogateProjectionKey1), LongManifest, "1", mergeable = true),
          SingleOffset(ProjectionId(projectionName, surrogateProjectionKey2), LongManifest, "2", mergeable = true)))

      fromStorageRepresentation[MergeableOffset[Long], Long](storageRepresentation) shouldBe mergeableOffset
    }
  }
}
