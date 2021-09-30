/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.Instant
import java.util.UUID

import scala.concurrent.ExecutionContext

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.Sequence
import akka.persistence.query.TimeBasedUUID
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.r2dbc.internal.R2dbcOffsetStore
import org.scalatest.wordspec.AnyWordSpecLike

class R2dbcOffsetStoreSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  // test clock for testing of the `last_updated` Instant
  private val clock = new TestClock

  private val settings = R2dbcProjectionSettings(testKit.system)
  private val offsetStore = new R2dbcOffsetStore(system, settings, r2dbcExecutor, clock)

  private val table = settings.tableWithSchema

  private implicit val ec: ExecutionContext = system.executionContext

  def selectLastSql: String =
    s"""SELECT * FROM $table WHERE projection_name = $$1 AND projection_key = $$2"""

  private def selectLastUpdated(projectionId: ProjectionId): Instant = {
    r2dbcExecutor
      .selectOne("test")(
        conn =>
          conn
            .createStatement(selectLastSql)
            .bind("$1", projectionId.name)
            .bind("$2", projectionId.key),
        row => Instant.ofEpochMilli(row.get("last_updated", classOf[java.lang.Long])))
      .futureValue
      .getOrElse(throw new RuntimeException(s"no records found for $projectionId"))
  }

  "The R2dbcOffsetStore" must {

    s"not fail when dropIfExists and createIfNotExists are called" in {
      pending // FIXME not implemented yet
      val dropAndCreate =
        for {
          _ <- offsetStore.dropIfExists()
          _ <- offsetStore.createIfNotExists()
        } yield Done
      dropAndCreate.futureValue
    }

    s"create and update offsets" in {
      val projectionId = genRandomProjectionId()

      offsetStore.saveOffset(projectionId, 1L).futureValue
      val offset1 = offsetStore.readOffset[Long](projectionId)
      offset1.futureValue shouldBe Some(1L)

      offsetStore.saveOffset(projectionId, 2L).futureValue
      val offset2 = offsetStore.readOffset[Long](projectionId)
      offset2.futureValue shouldBe Some(2L) // yep, saveOffset overwrites previous
    }

    s"save and retrieve offsets of type Long" in {
      val projectionId = genRandomProjectionId()
      offsetStore.saveOffset(projectionId, 1L).futureValue
      val offset = offsetStore.readOffset[Long](projectionId)
      offset.futureValue shouldBe Some(1L)
    }

    s"save and retrieve offsets of type java.lang.Long" in {
      val projectionId = genRandomProjectionId()
      offsetStore.saveOffset(projectionId, java.lang.Long.valueOf(1L)).futureValue
      val offset = offsetStore.readOffset[java.lang.Long](projectionId)
      offset.futureValue shouldBe Some(1L)
    }

    s"save and retrieve offsets of type Int" in {
      val projectionId = genRandomProjectionId()
      offsetStore.saveOffset(projectionId, 1).futureValue
      val offset = offsetStore.readOffset[Int](projectionId)
      offset.futureValue shouldBe Some(1)
    }

    s"save and retrieve offsets of type java.lang.Integer" in {
      val projectionId = genRandomProjectionId()
      offsetStore.saveOffset(projectionId, java.lang.Integer.valueOf(1)).futureValue
      val offset = offsetStore.readOffset[java.lang.Integer](projectionId)
      offset.futureValue shouldBe Some(1)
    }

    s"save and retrieve offsets of type String" in {
      val projectionId = genRandomProjectionId()
      val randOffset = UUID.randomUUID().toString
      offsetStore.saveOffset(projectionId, randOffset).futureValue
      val offset = offsetStore.readOffset[String](projectionId)
      offset.futureValue shouldBe Some(randOffset)
    }

    s"save and retrieve offsets of type akka.persistence.query.Sequence" in {
      val projectionId = genRandomProjectionId()
      val seqOffset = Sequence(1L)
      offsetStore.saveOffset(projectionId, seqOffset).futureValue
      val offset = offsetStore.readOffset[Sequence](projectionId)
      offset.futureValue shouldBe Some(seqOffset)
    }

    s"save and retrieve offsets of type akka.persistence.query.TimeBasedUUID" in {
      val projectionId = genRandomProjectionId()
      val timeUuidOffset =
        TimeBasedUUID(UUID.fromString("49225740-2019-11ea-a752-ffae2393b6e4")) //2019-12-16T15:32:36.148Z[UTC]
      offsetStore.saveOffset(projectionId, timeUuidOffset).futureValue
      val offset = offsetStore.readOffset[TimeBasedUUID](projectionId)
      offset.futureValue shouldBe Some(timeUuidOffset)
    }

    s"save and retrieve MergeableOffset" in {
      val projectionId = genRandomProjectionId()
      val origOffset = MergeableOffset(Map("abc" -> 1L, "def" -> 1L, "ghi" -> 1L))
      offsetStore.saveOffset(projectionId, origOffset).futureValue
      val offset = offsetStore.readOffset[MergeableOffset[Long]](projectionId)
      offset.futureValue shouldBe Some(origOffset)
    }

    s"add new offsets to MergeableOffset" in {
      val projectionId = genRandomProjectionId()

      val origOffset = MergeableOffset(Map("abc" -> 1L, "def" -> 1L))
      offsetStore.saveOffset(projectionId, origOffset).futureValue

      val offset1 = offsetStore.readOffset[MergeableOffset[Long]](projectionId)
      offset1.futureValue shouldBe Some(origOffset)

      // mix updates and inserts
      val updatedOffset = MergeableOffset(Map("abc" -> 2L, "def" -> 2L, "ghi" -> 1L))
      offsetStore.saveOffset(projectionId, updatedOffset).futureValue

      val offset2 = offsetStore.readOffset[MergeableOffset[Long]](projectionId)
      offset2.futureValue shouldBe Some(updatedOffset)
    }

    s"update timestamp" in {
      val projectionId = genRandomProjectionId()

      val instant0 = clock.instant()
      offsetStore.saveOffset(projectionId, 15).futureValue

      val instant1 = selectLastUpdated(projectionId)
      instant1 shouldBe instant0

      val instant2 = clock.tick(java.time.Duration.ofMillis(5))
      offsetStore.saveOffset(projectionId, 16).futureValue

      val instant3 = selectLastUpdated(projectionId)
      instant3 shouldBe instant2
    }

    s"clear offset" in {
      val projectionId = genRandomProjectionId()

      offsetStore.saveOffset(projectionId, 3L).futureValue
      offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(3L)

      offsetStore.clearOffset(projectionId).futureValue
      offsetStore.readOffset[Long](projectionId).futureValue shouldBe None
    }

    s"read and save paused" in {
      pending // FIXME not implemented yet
      val projectionId = genRandomProjectionId()

      offsetStore.readManagementState(projectionId).futureValue shouldBe None

      offsetStore.savePaused(projectionId, paused = true).futureValue
      offsetStore.readManagementState(projectionId).futureValue shouldBe Some(ManagementState(paused = true))

      offsetStore.savePaused(projectionId, paused = false).futureValue
      offsetStore.readManagementState(projectionId).futureValue shouldBe Some(ManagementState(paused = false))
    }
  }
}
