/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.r2dbc

import java.time.Instant
import java.util.UUID

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.TimestampOffset
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.r2dbc.internal.OffsetPidSeqNr
import akka.projection.r2dbc.internal.R2dbcOffsetStore
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object R2dbcOffsetStoreSingleConnectionSpec {
  // with a single connection any nested connection acquisition while saving fails with acquire timeout
  val config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc.connection-factory {
      initial-size = 0
      max-size = 1
      acquire-retry = 0
    }
    """)
    .withFallback(TestConfig.config)

  class TestSourceProvider(override val minSlice: Int, override val maxSlice: Int) extends BySlicesSourceProvider
}

class R2dbcOffsetStoreSingleConnectionSpec
    extends ScalaTestWithActorTestKit(R2dbcOffsetStoreSingleConnectionSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import R2dbcOffsetStoreSingleConnectionSpec.TestSourceProvider

  override def typedSystem: ActorSystem[_] = system

  private val settings = R2dbcProjectionSettings(testKit.system)

  private def createOffsetStore(projectionId: ProjectionId) =
    new R2dbcOffsetStore(
      projectionId,
      UUID.randomUUID().toString,
      Some(new TestSourceProvider(0, persistenceExt.numberOfSlices - 1)),
      system,
      settings,
      r2dbcExecutor)

  private def offset(pid: String, seqNr: Long, timestamp: Instant): OffsetPidSeqNr =
    OffsetPidSeqNr(TimestampOffset(timestamp, Map(pid -> seqNr)), pid, seqNr)

  "The R2dbcOffsetStore with a single connection in the pool" must {

    "save offset of pid that is not in the cache" in {
      val offsetStore = createOffsetStore(genRandomProjectionId())
      val pid = nextPid(nextEntityType())

      offsetStore.saveOffset(offset(pid, 1L, Instant.now())).futureValue
      offsetStore.getState().byPid(pid).seqNr shouldBe 1L
    }

    "save offsets of pids that are not in the cache" in {
      val offsetStore = createOffsetStore(genRandomProjectionId())
      val entityType = nextEntityType()
      val pids = (1 to 3).map(_ => nextPid(entityType))
      val timestamp = Instant.now()

      offsetStore.saveOffsets(pids.map(pid => offset(pid, 1L, timestamp))).futureValue
      pids.foreach(pid => offsetStore.getState().byPid(pid).seqNr shouldBe 1L)
    }

    "save offset in given transaction of pid that is not in the cache" in {
      val offsetStore = createOffsetStore(genRandomProjectionId())
      val pid = nextPid(nextEntityType())

      r2dbcExecutor
        .withConnection("test") { conn =>
          offsetStore.saveOffsetInTx(conn, offset(pid, 1L, Instant.now()))
        }
        .futureValue
      offsetStore.getState().byPid(pid).seqNr shouldBe 1L
    }

    "save offset of pid stored by other offset store instance" in {
      val projectionId = genRandomProjectionId()
      val pid = nextPid(nextEntityType())
      val timestamp = Instant.now()

      createOffsetStore(projectionId).saveOffset(offset(pid, 1L, timestamp)).futureValue

      val offsetStore = createOffsetStore(projectionId)
      offsetStore.saveOffset(offset(pid, 2L, timestamp.plusMillis(1))).futureValue
      offsetStore.getState().byPid(pid).seqNr shouldBe 2L
    }

    "save offsets of pids stored by other offset store instance" in {
      val projectionId = genRandomProjectionId()
      val entityType = nextEntityType()
      val p1 = nextPid(entityType)
      val p2 = nextPid(entityType)
      val p3 = nextPid(entityType)
      val timestamp = Instant.now()

      val otherOffsetStore = createOffsetStore(projectionId)
      otherOffsetStore.saveOffset(offset(p1, 1L, timestamp)).futureValue
      otherOffsetStore.saveOffset(offset(p1, 2L, timestamp.plusMillis(1))).futureValue
      otherOffsetStore.saveOffset(offset(p2, 1L, timestamp.plusMillis(2))).futureValue

      val offsetStore = createOffsetStore(projectionId)
      offsetStore
        .saveOffsets(
          Vector(
            offset(p1, 3L, timestamp.plusMillis(3)),
            // duplicate
            offset(p2, 1L, timestamp.plusMillis(2)),
            offset(p3, 1L, timestamp.plusMillis(4))))
        .futureValue
      offsetStore.getState().byPid(p1).seqNr shouldBe 3L
      offsetStore.getState().byPid(p2).seqNr shouldBe 1L
      offsetStore.getState().byPid(p3).seqNr shouldBe 1L
    }
  }
}
