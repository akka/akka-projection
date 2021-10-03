/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.Instant
import java.time.{ Duration => JDuration }

import scala.concurrent.ExecutionContext

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.query.TimestampOffset
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.r2dbc.internal.R2dbcOffsetStore
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

class R2dbcTimestampOffsetStoreSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val clock = new TestClock
  def tick(): Unit = clock.tick(JDuration.ofMillis(1))

  private val log = LoggerFactory.getLogger(getClass)

  private val settings = R2dbcProjectionSettings(testKit.system)

  private def createOffsetStore(projectionId: ProjectionId, customSettings: R2dbcProjectionSettings = settings) =
    new R2dbcOffsetStore(projectionId, system, customSettings, r2dbcExecutor)

  private val table = settings.timestampOffsetTableWithSchema

  private implicit val ec: ExecutionContext = system.executionContext

  "The R2dbcOffsetStore for TimestampOffset" must {

    s"update TimestampOffset with one entry" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L))
      offsetStore.saveOffset(offset1).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffset]()
      readOffset1.futureValue shouldBe Some(offset1)

      tick()
      val offset2 = TimestampOffset(clock.instant(), Map("p1" -> 4L))
      offsetStore.saveOffset(offset2).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffset]()
      readOffset2.futureValue shouldBe Some(offset2) // yep, saveOffset overwrites previous
    }

    s"update TimestampOffset with several entries" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L, "p2" -> 1L, "p3" -> 5L))
      offsetStore.saveOffset(offset1).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffset]()
      readOffset1.futureValue shouldBe Some(offset1)

      tick()
      val offset2 = TimestampOffset(clock.instant(), Map("p1" -> 4L, "p3" -> 6L, "p4" -> 9L))
      offsetStore.saveOffset(offset2).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffset]()
      // p2 is not included in read offset because it wasn't updated and has earlier timestamp
      readOffset2.futureValue shouldBe Some(offset2)
    }

    s"update TimestampOffset when same timestamp" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L, "p2" -> 1L, "p3" -> 5L))
      offsetStore.saveOffset(offset1).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffset]()
      readOffset1.futureValue shouldBe Some(offset1)

      // not tick, same timestamp
      val offset2 = TimestampOffset(clock.instant(), Map("p2" -> 2L, "p4" -> 9L))
      offsetStore.saveOffset(offset2).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffset]()
      // all should be included since same timestamp
      val expectedOffset2 = TimestampOffset(clock.instant(), Map("p1" -> 3L, "p2" -> 2L, "p3" -> 5L, "p4" -> 9L))
      readOffset2.futureValue shouldBe Some(expectedOffset2)

      // saving new with later timestamp
      tick()
      val offset3 = TimestampOffset(clock.instant(), Map("p1" -> 4L))
      offsetStore.saveOffset(offset3).futureValue
      val readOffset3 = offsetStore.readOffset[TimestampOffset]()
      // then it should only contain that entry
      readOffset3.futureValue shouldBe Some(offset3)
    }

    s"not update when earlier seqNr" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L))
      offsetStore.saveOffset(offset1).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffset]()
      readOffset1.futureValue shouldBe Some(offset1)

      clock.setInstant(clock.instant().minusMillis(1))
      val offset2 = TimestampOffset(clock.instant(), Map("p1" -> 2L))
      offsetStore.saveOffset(offset2).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffset]()
      readOffset2.futureValue shouldBe Some(offset1) // keeping offset1
    }

    s"evict old records" in {
      val projectionId = genRandomProjectionId()
      val evictSettings = settings.copy(timeWindow = JDuration.ofSeconds(100), evictInterval = JDuration.ofSeconds(10))
      import evictSettings._
      val offsetStore = createOffsetStore(projectionId, evictSettings)

      val startTime = Instant.now()
      log.debug("Start time [{}]", startTime)

      offsetStore.saveOffset(TimestampOffset(startTime, Map("p1" -> 1L))).futureValue
      offsetStore.saveOffset(TimestampOffset(startTime.plus(JDuration.ofSeconds(1)), Map("p2" -> 1L))).futureValue
      offsetStore.saveOffset(TimestampOffset(startTime.plus(JDuration.ofSeconds(2)), Map("p3" -> 1L))).futureValue
      offsetStore.saveOffset(TimestampOffset(startTime.plus(evictInterval), Map("p4" -> 1L))).futureValue
      offsetStore
        .saveOffset(TimestampOffset(startTime.plus(evictInterval).plus(JDuration.ofSeconds(1)), Map("p4" -> 1L)))
        .futureValue
      offsetStore
        .saveOffset(TimestampOffset(startTime.plus(evictInterval).plus(JDuration.ofSeconds(2)), Map("p5" -> 1L)))
        .futureValue
      offsetStore
        .saveOffset(TimestampOffset(startTime.plus(evictInterval).plus(JDuration.ofSeconds(3)), Map("p6" -> 1L)))
        .futureValue
      offsetStore.getState().size shouldBe 6

      offsetStore.saveOffset(TimestampOffset(startTime.plus(timeWindow.minusSeconds(10)), Map("p7" -> 1L))).futureValue
      offsetStore.getState().size shouldBe 7 // nothing evicted yet

      offsetStore
        .saveOffset(TimestampOffset(startTime.plus(timeWindow.plus(evictInterval).minusSeconds(3)), Map("p8" -> 1L)))
        .futureValue
      offsetStore.getState().size shouldBe 8 // still nothing evicted yet

      offsetStore
        .saveOffset(TimestampOffset(startTime.plus(timeWindow.plus(evictInterval).plusSeconds(1)), Map("p8" -> 2L)))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set("p5", "p6", "p7", "p8")

      offsetStore
        .saveOffset(TimestampOffset(startTime.plus(timeWindow.plus(evictInterval).plusSeconds(20)), Map("p8" -> 3L)))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set("p7", "p8")
    }

    s"delete old records" in {
      val projectionId = genRandomProjectionId()
      val deleteSettings = settings.copy(timeWindow = JDuration.ofSeconds(100))
      import deleteSettings._
      val offsetStore = createOffsetStore(projectionId, deleteSettings)

      val startTime = Instant.now()
      log.debug("Start time [{}]", startTime)

      offsetStore.saveOffset(TimestampOffset(startTime, Map("p1" -> 1L))).futureValue
      offsetStore.saveOffset(TimestampOffset(startTime.plus(JDuration.ofSeconds(1)), Map("p2" -> 1L))).futureValue
      offsetStore.saveOffset(TimestampOffset(startTime.plus(JDuration.ofSeconds(2)), Map("p3" -> 1L))).futureValue
      offsetStore.saveOffset(TimestampOffset(startTime.plus(JDuration.ofSeconds(3)), Map("p4" -> 1L))).futureValue
      offsetStore.deleteOldTimestampOffsets().futureValue shouldBe 0
      offsetStore.readOffset().futureValue // this will load from database
      offsetStore.getState().size shouldBe 4

      offsetStore.saveOffset(TimestampOffset(startTime.plus(timeWindow.minusSeconds(2)), Map("p5" -> 1L))).futureValue
      offsetStore.saveOffset(TimestampOffset(startTime.plus(timeWindow.minusSeconds(1)), Map("p6" -> 1L))).futureValue
      // nothing deleted yet
      offsetStore.deleteOldTimestampOffsets().futureValue shouldBe 0
      offsetStore.readOffset().futureValue // this will load from database
      offsetStore.getState().size shouldBe 6

      offsetStore.saveOffset(TimestampOffset(startTime.plus(timeWindow.plusSeconds(1)), Map("p7" -> 1L))).futureValue
      offsetStore.saveOffset(TimestampOffset(startTime.plus(timeWindow.plusSeconds(2)), Map("p8" -> 1L))).futureValue
      offsetStore.deleteOldTimestampOffsets().futureValue shouldBe 2
      offsetStore.readOffset().futureValue // this will load from database
      offsetStore.getState().byPid.keySet shouldBe Set("p3", "p4", "p5", "p6", "p7", "p8")
    }

    s"periodically delete old records" in {
      val projectionId = genRandomProjectionId()
      val deleteSettings =
        settings.copy(timeWindow = JDuration.ofSeconds(100), deleteInterval = JDuration.ofMillis(500))
      import deleteSettings._
      val offsetStore = createOffsetStore(projectionId, deleteSettings)

      val startTime = Instant.now()
      log.debug("Start time [{}]", startTime)

      offsetStore.saveOffset(TimestampOffset(startTime, Map("p1" -> 1L))).futureValue
      offsetStore.saveOffset(TimestampOffset(startTime.plus(timeWindow.plusSeconds(1)), Map("p2" -> 1L))).futureValue
      eventually {
        offsetStore.readOffset().futureValue // this will load from database
        offsetStore.getState().byPid.keySet shouldBe Set("p2")
      }

      offsetStore
        .saveOffset(TimestampOffset(startTime.plus(timeWindow.multipliedBy(2).plusSeconds(2)), Map("p3" -> 1L)))
        .futureValue
      eventually {
        offsetStore.readOffset().futureValue // this will load from database
        offsetStore.getState().byPid.keySet shouldBe Set("p3")
      }
    }

    s"clear offset" in {
      pending // FIXME not implemented yet
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      offsetStore.saveOffset(3L).futureValue
      offsetStore.readOffset[Long]().futureValue shouldBe Some(3L)

      offsetStore.clearOffset().futureValue
      offsetStore.readOffset[Long]().futureValue shouldBe None
    }

    s"read and save paused" in {
      pending // FIXME not implemented yet
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
