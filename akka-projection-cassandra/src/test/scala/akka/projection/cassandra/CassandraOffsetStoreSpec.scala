/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra

import java.time.Instant
import java.util.UUID

import scala.concurrent.ExecutionContext
import scala.util.Try

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.Sequence
import akka.persistence.query.TimeBasedUUID
import akka.projection.ProjectionId
import akka.projection.cassandra.internal.CassandraOffsetStore
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import org.scalatest.wordspec.AnyWordSpecLike

class CassandraOffsetStoreSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  private val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra")
  private implicit val ec: ExecutionContext = system.executionContext
  private val offsetStore = new CassandraOffsetStore(session)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // reason for setSchemaMetadataEnabled is that it speed up tests
    session.underlying().map(_.setSchemaMetadataEnabled(false)).futureValue
    offsetStore.createKeyspaceAndTable().futureValue
    session.underlying().map(_.setSchemaMetadataEnabled(null)).futureValue
  }

  override protected def afterAll(): Unit = {
    Try(session.executeDDL(s"DROP keyspace ${offsetStore.keyspace}").futureValue)
    super.afterAll()
  }

  private def selectLastUpdated(projectionId: ProjectionId): Instant = {
    session
      .selectOne(
        s"select last_updated from ${offsetStore.keyspace}.${offsetStore.table} where projection_id = ?",
        projectionId.id)
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
      offsetStore.saveOffset(projectionId, 15)
      val instant1 = selectLastUpdated(projectionId)
      (System.currentTimeMillis() - instant1.toEpochMilli) should be >= 0L
      (System.currentTimeMillis() - instant1.toEpochMilli) should be < 3000L

      // probably no risk to hit same timestamp, but anyway
      Thread.sleep(10)

      offsetStore.saveOffset(projectionId, 16)
      val instant2 = selectLastUpdated(projectionId)
      (instant2.toEpochMilli - instant1.toEpochMilli) should be >= 0L
      (instant2.toEpochMilli - instant1.toEpochMilli) should be < 3000L
    }
  }
}
