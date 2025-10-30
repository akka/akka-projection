/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.cassandra

import java.time.Instant
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.FutureConverters._
import scala.util.Try

import akka.Done
import akka.actor.Scheduler
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.Sequence
import akka.persistence.query.TimeBasedUUID
import akka.projection.ProjectionId
import akka.projection.cassandra.internal.CassandraOffsetStore
import akka.projection.internal.ManagementState
import akka.projection.testkit.internal.TestClock
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import org.scalatest.wordspec.AnyWordSpecLike

class CassandraOffsetStoreSpec
    extends ScalaTestWithActorTestKit(ContainerSessionProvider.Config)
    with AnyWordSpecLike
    with LogCapturing {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 10.seconds, interval = 100.millis)

  // test clock for testing of the `last_updated` Instant
  private val clock = new TestClock
  private val offsetStore = new CassandraOffsetStore(system, clock)
  private val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra.session-config")
  private implicit val ec: ExecutionContext = system.executionContext
  private implicit val classicScheduler: Scheduler = system.classicSystem.scheduler

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // don't use futureValue (patience) here because it can take a while to start the test container
    Await.result(ContainerSessionProvider.started, 30.seconds)

    def tryCreateTable() =
      for {
        s <- session.underlying()

        // reason for setSchemaMetadataEnabled is that it speed up tests
        _ <- s.setSchemaMetadataEnabled(false).asScala
        _ <- offsetStore.createKeyspaceAndTable()
        _ <- s.setSchemaMetadataEnabled(null).asScala
      } yield Done

    // the container can takes time to be 'ready',
    // we should keep trying to create the table until it succeeds
    Await.result(akka.pattern.retry(() => tryCreateTable(), 20, 3.seconds), 60.seconds)
  }

  override protected def afterAll(): Unit = {
    Try(session.executeDDL(s"DROP keyspace ${offsetStore.keyspace}").futureValue)
    super.afterAll()
  }

  private def selectLastUpdated(projectionId: ProjectionId): Instant = {
    session
      .selectOne(
        s"select projection_key, last_updated from ${offsetStore.keyspace}.${offsetStore.table} where projection_name = ? AND partition = ? AND projection_key = ?",
        projectionId.name,
        offsetStore.idToPartition(projectionId),
        projectionId.key)
      .futureValue
      .get
      .get("last_updated", classOf[Instant])
  }

  "The Cassandra OffsetStore" must {

    "create and update offsets" in {
      val projectionId = ProjectionId("projection-with-long", "00")

      withClue("check - save offset 1L") {
        offsetStore.saveOffset(projectionId, 1L).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 1L
      }

      withClue("check - save offset 2L") {
        offsetStore.saveOffset(projectionId, 2L).futureValue
      }

      withClue("check - read offset after overwrite") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 2L // yep, saveOffset overwrites previous
      }

    }

    "save and retrieve offsets of type Long" in {
      val projectionId = ProjectionId("projection-with-long", "00")

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 1L).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 1L
      }

    }

    "save and retrieve offsets of type java.lang.Long" in {
      val projectionId = ProjectionId("projection-with-java-long", "00")

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, java.lang.Long.valueOf(1L)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Long](projectionId).futureValue.get
        offset shouldBe 1L
      }
    }

    "save and retrieve offsets of type Int" in {
      val projectionId = ProjectionId("projection-with-int", "00")

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 1).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Int](projectionId).futureValue.get
        offset shouldBe 1
      }

    }

    "save and retrieve offsets of type java.lang.Integer" in {
      val projectionId = ProjectionId("projection-with-java-int", "00")

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, java.lang.Integer.valueOf(1)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Integer](projectionId).futureValue.get
        offset shouldBe 1
      }
    }

    "save and retrieve offsets of type String" in {

      val projectionId = ProjectionId("projection-with-String", "00")

      val randOffset = UUID.randomUUID().toString
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, randOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[String](projectionId).futureValue.get
        offset shouldBe randOffset
      }
    }

    "save and retrieve offsets of type akka.persistence.query.Sequence" in {

      val projectionId = ProjectionId("projection-with-akka-seq", "00")

      val seqOffset = Sequence(1L)
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, seqOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Sequence](projectionId).futureValue.get
        offset shouldBe seqOffset
      }
    }

    "save and retrieve offsets of type akka.persistence.query.TimeBasedUUID" in {

      val projectionId = ProjectionId("projection-with-akka-seq", "00")

      val timeOffset = TimeBasedUUID(UUID.fromString("49225740-2019-11ea-a752-ffae2393b6e4")) //2019-12-16T15:32:36.148Z[UTC]
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, timeOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[TimeBasedUUID](projectionId).futureValue.get
        offset shouldBe timeOffset
      }
    }

    "update timestamp" in {
      val projectionId = ProjectionId("timestamp", "00")

      val instant0 = clock.instant()
      offsetStore.saveOffset(projectionId, 15).futureValue
      val instant1 = selectLastUpdated(projectionId)
      instant1 shouldBe instant0

      val instant2 = clock.tick(java.time.Duration.ofMillis(5))
      offsetStore.saveOffset(projectionId, 16).futureValue
      val instant3 = selectLastUpdated(projectionId)
      instant3 shouldBe instant2
    }

    "clear offset" in {
      val projectionId = ProjectionId("projection-clear", "00")

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 3L).futureValue
      }

      withClue("check - read offset") {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(3)
      }

      withClue("check - clear") {
        offsetStore.clearOffset(projectionId).futureValue
      }

      withClue("check - read offset") {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe None
      }
    }

    "read and save paused" in {
      val projectionId = ProjectionId("projection-pause", "00")

      offsetStore.readManagementState(projectionId).futureValue shouldBe None

      offsetStore.savePaused(projectionId, paused = true).futureValue
      offsetStore.readManagementState(projectionId).futureValue shouldBe Some(ManagementState(paused = true))

      offsetStore.savePaused(projectionId, paused = false).futureValue
      offsetStore.readManagementState(projectionId).futureValue shouldBe Some(ManagementState(paused = false))
    }
  }
}
