/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.util.UUID

import akka.persistence.query
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OffsetSerializationSpec extends TestSuite with Matchers with AnyWordSpecLike {
  import OffsetSerialization._

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
  }

  "OffsetSerialization of MergeableOffsets" must {
    "convert offsets of type MergeableOffset" in {
      val surrogateProjectionKey = "user-group-topic-1"
      val mergeableOffset = MergeableOffset(Map(surrogateProjectionKey -> 1L))
      val actualRep = toStorageRepresentation(id, mergeableOffset)

      withClue("return a storage representation of MultipleOffsets") {
        actualRep.getClass shouldBe classOf[MultipleOffsets]
      }

      withClue("override provided projection key with surrogate projection key") {
        actualRep.asInstanceOf[MultipleOffsets].reps.head.id.key shouldBe surrogateProjectionKey
      }

      val storageRepresentation = MultipleOffsets(
        Seq(SingleOffset(ProjectionId(id.name, surrogateProjectionKey), LongManifest, "1", mergeable = true)))

      actualRep shouldBe storageRepresentation

      fromStorageRepresentation[MergeableOffset[Long], Long](storageRepresentation) shouldBe mergeableOffset
    }

    "merge rows into one MergeableOffset" in {
      val projectionName = "user-projection"
      val surrogateProjectionKey1 = "user-group-topic-1"
      val surrogateProjectionKey2 = "user-group-topic-2"

      val mergeableOffset = MergeableOffset(Map(surrogateProjectionKey1 -> 1L, surrogateProjectionKey2 -> 2L))
      val storageRepresentation = MultipleOffsets(
        Seq(
          SingleOffset(ProjectionId(projectionName, surrogateProjectionKey1), LongManifest, "1", mergeable = true),
          SingleOffset(ProjectionId(projectionName, surrogateProjectionKey2), LongManifest, "2", mergeable = true)))

      fromStorageRepresentation[MergeableOffset[Long], Long](storageRepresentation) shouldBe mergeableOffset
    }
  }
}
