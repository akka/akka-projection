/*
 * Copyright (C) 2026 Lightbend Inc. <https://akka.io>
 */

package akka.projection.r2dbc

import java.time.Instant
import java.util.UUID

import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.TimestampOffset
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.r2dbc.R2dbcTimestampOffsetStoreSpec.TestTimestampSourceProvider
import akka.projection.r2dbc.internal.OffsetPidSeqNr
import akka.projection.r2dbc.internal.R2dbcOffsetStore
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import org.scalatest.wordspec.AnyWordSpecLike

object H2SchemaOffsetStoreSpec {
  val schema = "projection_schema"

  // the projection tables only exist in the schema, so any statement not using it fails
  val config: Config = {
    val base = ConfigFactory
      .parseString(s"""
        akka.persistence.r2dbc.connection-factory = $${akka.persistence.r2dbc.h2}
        akka.persistence.r2dbc.connection-factory {
          protocol = "mem"
          database = "h2-schema-offset-store-spec-${UUID.randomUUID()}"
        }
        akka.projection.r2dbc.offset-store.schema = "$schema"
        """)
      .withFallback(ConfigFactory.parseResources("h2-default-projection-schema.conf"))
      .withFallback(TestConfig.config)
      .resolve()
    val schemaDdl = s"CREATE SCHEMA IF NOT EXISTS $schema;\n" +
      base.getString("akka.projection.r2dbc.default-h2-schema").replace("akka_projection_", s"$schema.akka_projection_")
    base.withValue(
      "akka.persistence.r2dbc.connection-factory.additional-init",
      ConfigValueFactory.fromAnyRef(schemaDdl))
  }
}

class H2SchemaOffsetStoreSpec
    extends ScalaTestWithActorTestKit(H2SchemaOffsetStoreSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val clock = TestClock.nowMicros()

  private val settings = R2dbcProjectionSettings(testKit.system)

  private def createOffsetStore(projectionId: ProjectionId, customSettings: R2dbcProjectionSettings = settings) =
    new R2dbcOffsetStore(
      projectionId,
      UUID.randomUUID().toString,
      Some(new TestTimestampSourceProvider(0, persistenceExt.numberOfSlices - 1, clock)),
      system,
      customSettings,
      r2dbcExecutor,
      clock)

  "The R2dbcOffsetStore with H2 and a custom schema" must {

    "save and read primitive offsets" in {
      val offsetStore = createOffsetStore(genRandomProjectionId())

      offsetStore.saveOffset(OffsetPidSeqNr(1L)).futureValue
      offsetStore.readOffset[Long]().futureValue shouldBe Some(1L)

      offsetStore.saveOffset(OffsetPidSeqNr(2L)).futureValue
      offsetStore.readOffset[Long]().futureValue shouldBe Some(2L)
    }

    "save and read paused" in {
      val offsetStore = createOffsetStore(genRandomProjectionId())

      offsetStore.readManagementState().futureValue shouldBe None
      offsetStore.savePaused(paused = true).futureValue
      offsetStore.readManagementState().futureValue shouldBe Some(ManagementState(paused = true))
      offsetStore.savePaused(paused = false).futureValue
      offsetStore.readManagementState().futureValue shouldBe Some(ManagementState(paused = false))
    }

    "save, read and delete timestamp offsets" in {
      val deleteSettings = settings
        .withTimeWindow(100.seconds)
        .withDeleteAfter(100.seconds)
        .withDeleteAfterConsumed(1.millis)
      val offsetStore = createOffsetStore(genRandomProjectionId(), deleteSettings)

      // same slice, otherwise the latest for each slice is kept
      val p1 = "p500"
      val p2 = "p621"
      val startTime = Instant.now()
      val offset1 = TimestampOffset(startTime, Map(p1 -> 1L))
      val offset2 = TimestampOffset(startTime.plus(deleteSettings.deleteAfter).plusSeconds(1), Map(p2 -> 1L))

      offsetStore.saveOffset(OffsetPidSeqNr(offset1, p1, 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, p2, 1L)).futureValue
      offsetStore.readOffset[TimestampOffset]().futureValue shouldBe Some(offset2)

      // make sure timestamp_consumed is older than delete-after-consumed
      Thread.sleep(50)
      offsetStore.deleteOldTimestampOffsets().futureValue shouldBe 1
      offsetStore.readOffset[TimestampOffset]().futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p2)
    }
  }
}
