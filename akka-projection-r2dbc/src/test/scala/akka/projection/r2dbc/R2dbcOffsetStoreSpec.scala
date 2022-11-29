/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.Instant
import java.util.UUID

import scala.concurrent.ExecutionContext

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.Sequence
import akka.persistence.query.TimeBasedUUID
import akka.persistence.r2dbc.internal.Sql.Interpolation
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
  private val clock = TestClock.nowMillis()

  private val settings = R2dbcProjectionSettings(testKit.system)

  private def createOffsetStore(projectionId: ProjectionId) =
    new R2dbcOffsetStore(projectionId, None, system, settings, r2dbcExecutor, clock)

  private val table = settings.offsetTableWithSchema

  private implicit val ec: ExecutionContext = system.executionContext

  def selectLastSql: String =
    sql"SELECT * FROM $table WHERE projection_name = ? AND projection_key = ?"

  private def selectLastUpdated(projectionId: ProjectionId): Instant = {
    r2dbcExecutor
      .selectOne("test")(
        conn =>
          conn
            .createStatement(selectLastSql)
            .bind(0, projectionId.name)
            .bind(1, projectionId.key),
        row => Instant.ofEpochMilli(row.get("last_updated", classOf[java.lang.Long])))
      .futureValue
      .getOrElse(throw new RuntimeException(s"no records found for $projectionId"))
  }

  "The R2dbcOffsetStore" must {

    s"save and read offsets" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      offsetStore.saveOffset(1L).futureValue
      val offset1 = offsetStore.readOffset[Long]()
      offset1.futureValue shouldBe Some(1L)

      offsetStore.saveOffset(2L).futureValue
      val offset2 = offsetStore.readOffset[Long]()
      offset2.futureValue shouldBe Some(2L) // yep, saveOffset overwrites previous
    }

    s"save and retrieve offsets of type Long" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      offsetStore.saveOffset(1L).futureValue
      val offset = offsetStore.readOffset[Long]()
      offset.futureValue shouldBe Some(1L)
    }

    s"save and retrieve offsets of type java.lang.Long" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      offsetStore.saveOffset(java.lang.Long.valueOf(1L)).futureValue
      val offset = offsetStore.readOffset[java.lang.Long]()
      offset.futureValue shouldBe Some(1L)
    }

    s"save and retrieve offsets of type Int" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      offsetStore.saveOffset(1).futureValue
      val offset = offsetStore.readOffset[Int]()
      offset.futureValue shouldBe Some(1)
    }

    s"save and retrieve offsets of type java.lang.Integer" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      offsetStore.saveOffset(java.lang.Integer.valueOf(1)).futureValue
      val offset = offsetStore.readOffset[java.lang.Integer]()
      offset.futureValue shouldBe Some(1)
    }

    s"save and retrieve offsets of type String" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      val randOffset = UUID.randomUUID().toString
      offsetStore.saveOffset(randOffset).futureValue
      val offset = offsetStore.readOffset[String]()
      offset.futureValue shouldBe Some(randOffset)
    }

    s"save and retrieve offsets of type akka.persistence.query.Sequence" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      val seqOffset = Sequence(1L)
      offsetStore.saveOffset(seqOffset).futureValue
      val offset = offsetStore.readOffset[Sequence]()
      offset.futureValue shouldBe Some(seqOffset)
    }

    s"save and retrieve offsets of type akka.persistence.query.TimeBasedUUID" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val timeUuidOffset =
        TimeBasedUUID(UUID.fromString("49225740-2019-11ea-a752-ffae2393b6e4")) //2019-12-16T15:32:36.148Z[UTC]
      offsetStore.saveOffset(timeUuidOffset).futureValue
      val offset = offsetStore.readOffset[TimeBasedUUID]()
      offset.futureValue shouldBe Some(timeUuidOffset)
    }

    s"save and retrieve offsets of unknown custom serializable type" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val customOffset = "abc"
      offsetStore.saveOffset(customOffset).futureValue
      val offset = offsetStore.readOffset[TimeBasedUUID]()
      offset.futureValue shouldBe Some(customOffset)
    }

    s"save and retrieve MergeableOffset" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      val origOffset = MergeableOffset(Map("abc" -> 1L, "def" -> 1L, "ghi" -> 1L))
      offsetStore.saveOffset(origOffset).futureValue
      val offset = offsetStore.readOffset[MergeableOffset[Long]]()
      offset.futureValue shouldBe Some(origOffset)
    }

    s"add new offsets to MergeableOffset" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val origOffset = MergeableOffset(Map("abc" -> 1L, "def" -> 1L))
      offsetStore.saveOffset(origOffset).futureValue

      val offset1 = offsetStore.readOffset[MergeableOffset[Long]]()
      offset1.futureValue shouldBe Some(origOffset)

      // mix updates and inserts
      val updatedOffset = MergeableOffset(Map("abc" -> 2L, "def" -> 2L, "ghi" -> 1L))
      offsetStore.saveOffset(updatedOffset).futureValue

      val offset2 = offsetStore.readOffset[MergeableOffset[Long]]()
      offset2.futureValue shouldBe Some(updatedOffset)
    }

    s"update timestamp" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val instant0 = clock.instant()
      offsetStore.saveOffset(15).futureValue

      val instant1 = selectLastUpdated(projectionId)
      instant1 shouldBe instant0

      val instant2 = clock.tick(java.time.Duration.ofMillis(5))
      offsetStore.saveOffset(16).futureValue

      val instant3 = selectLastUpdated(projectionId)
      instant3 shouldBe instant2
    }

    s"set offset" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      offsetStore.saveOffset(3L).futureValue
      offsetStore.readOffset[Long]().futureValue shouldBe Some(3L)

      offsetStore.managementSetOffset(2L).futureValue
      offsetStore.readOffset[Long]().futureValue shouldBe Some(2L)
    }

    s"clear offset" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      offsetStore.saveOffset(3L).futureValue
      offsetStore.readOffset[Long]().futureValue shouldBe Some(3L)

      offsetStore.managementClearOffset().futureValue
      offsetStore.readOffset[Long]().futureValue shouldBe None
    }

    s"read and save paused" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      offsetStore.readManagementState().futureValue shouldBe None

      offsetStore.savePaused(paused = true).futureValue
      offsetStore.readManagementState().futureValue shouldBe Some(ManagementState(paused = true))

      offsetStore.savePaused(paused = false).futureValue
      offsetStore.readManagementState().futureValue shouldBe Some(ManagementState(paused = false))
    }
  }
}
