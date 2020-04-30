/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.util.UUID

import akka.persistence.query
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OffsetSerializationSpec extends TestSuite with Matchers with AnyWordSpecLike {
  import OffsetSerialization._

  private val longValue = Long.MaxValue - 17

  "OffsetSerialization" must {
    "convert offsets of type Long" in {
      toStorageRepresentation(longValue) shouldBe Seq(longValue.toString -> LongManifest)
      fromStorageRepresentation[Long](longValue.toString, LongManifest) shouldBe longValue
    }

    "convert offsets of type java.lang.Long" in {
      toStorageRepresentation(
        java.lang.Long
          .valueOf(longValue)) shouldBe Seq(longValue.toString -> LongManifest)
      fromStorageRepresentation[java.lang.Long](longValue.toString, LongManifest) shouldBe
      java.lang.Long
        .valueOf(longValue)
    }

    "convert offsets of type Int" in {
      toStorageRepresentation(17) shouldBe Seq("17" -> IntManifest)
      fromStorageRepresentation[Int]("17", IntManifest) shouldBe 17
    }

    "convert offsets of type java.lang.Integer" in {
      toStorageRepresentation(java.lang.Integer.valueOf(17)) shouldBe Seq("17" -> IntManifest)
      fromStorageRepresentation[java.lang.Integer]("17", IntManifest) shouldBe java.lang.Integer.valueOf(17)
    }

    "convert offsets of type String" in {
      toStorageRepresentation("abc") shouldBe Seq("abc" -> StringManifest)
      fromStorageRepresentation[String]("abc", StringManifest) shouldBe "abc"
    }

    "convert offsets of type akka.persistence.query.Sequence" in {
      toStorageRepresentation(query.Sequence(1L)) shouldBe Seq("1" -> SequenceManifest)
      fromStorageRepresentation[query.Sequence]("1", SequenceManifest) shouldBe query.Sequence(1L)
    }

    "convert offsets of type akka.persistence.query.TimeBasedUUID" in {
      //2019-12-16T15:32:36.148Z[UTC]
      val uuidString = "49225740-2019-11ea-a752-ffae2393b6e4"
      val timeOffset = query.TimeBasedUUID(UUID.fromString(uuidString))
      toStorageRepresentation(timeOffset) shouldBe Seq(uuidString -> TimeBasedUUIDManifest)
      fromStorageRepresentation[query.TimeBasedUUID](uuidString, TimeBasedUUIDManifest) shouldBe timeOffset
    }

  }
}
