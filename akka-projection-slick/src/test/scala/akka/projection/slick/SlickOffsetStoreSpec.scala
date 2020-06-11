/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import java.time.Instant
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.Sequence
import akka.persistence.query.TimeBasedUUID
import akka.projection.ProjectionId
import akka.projection.slick.internal.SlickOffsetStore
import akka.projection.slick.internal.SlickSettings
import akka.projection.testkit.internal.TestClock
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.OptionValues
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import org.scalatest.wordspec.AnyWordSpecLike
import slick.basic.DatabaseConfig
import slick.jdbc.H2Profile

object SlickOffsetStoreSpec {
  def config: Config = ConfigFactory.parseString("""
    akka.projection.slick = {

      profile = "slick.jdbc.H2Profile$"

      # TODO: configure connection pool and slick async executor
      db = {
       url = "jdbc:h2:mem:offset-store-test;DB_CLOSE_DELAY=-1"
       driver = org.h2.Driver
       connectionPool = disabled
       keepAliveConnection = true
      }
      
      offset-store {
        schema = ""
        table = "AKKA_PROJECTION_OFFSET_STORE"
      }
    }
    """)
}
class SlickOffsetStoreSpec
    extends ScalaTestWithActorTestKit(SlickOffsetStoreSpec.config)
    with LogCapturing
    with AnyWordSpecLike
    with OptionValues {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(100, Millis))

  private val slickConfig = SlickOffsetStoreSpec.config.getConfig(SlickSettings.configPath)

  val dbConfig: DatabaseConfig[H2Profile] =
    DatabaseConfig.forConfig(SlickSettings.configPath, SlickOffsetStoreSpec.config)

  // test clock for testing of the `last_updated` Instant
  private val clock = new TestClock

  private val offsetStore =
    new SlickOffsetStore(dbConfig.db, dbConfig.profile, SlickSettings(slickConfig), clock)

  override protected def beforeAll(): Unit = {
    // create offset table
    Await.result(offsetStore.createIfNotExists, 3.seconds)
  }

  override protected def afterAll(): Unit = {
    dbConfig.db.close()
  }

  private def selectLastUpdated(projectionId: ProjectionId): Instant = {
    import dbConfig.profile.api._
    val action = offsetStore.offsetTable
      .filter(r => r.projectionName === projectionId.name && r.projectionKey === projectionId.key)
      .result
      .headOption
    dbConfig.db.run(action).futureValue.get.lastUpdated
  }

  "The SlickOffsetStore" must {

    implicit val ec: ExecutionContext = dbConfig.db.executor.executionContext

    "create and update offsets" in {

      val projectionId = ProjectionId("projection-with-long", "00")

      withClue("check - save offset 1L") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, 1L)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 1L
      }

      withClue("check - save offset 2L") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, 2L)).futureValue
      }

      withClue("check - read offset after overwrite") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 2L // yep, saveOffset overwrites previous
      }

    }

    "save and retrieve offsets of type Long" in {

      val projectionId = ProjectionId("projection-with-long", "00")

      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, 1L)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 1L
      }

    }

    "save and retrieve offsets of type java.lang.Long" in {

      val projectionId = ProjectionId("projection-with-java-long", "00")

      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, java.lang.Long.valueOf(1L))).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Long](projectionId).futureValue.value
        offset shouldBe 1L
      }
    }

    "save and retrieve offsets of type Int" in {

      val projectionId = ProjectionId("projection-with-int", "00")

      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, 1)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Int](projectionId).futureValue.value
        offset shouldBe 1
      }

    }

    "save and retrieve offsets of type java.lang.Integer" in {

      val projectionId = ProjectionId("projection-with-java-int", "00")

      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, java.lang.Integer.valueOf(1))).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Integer](projectionId).futureValue.value
        offset shouldBe 1
      }
    }

    "save and retrieve offsets of type String" in {

      val projectionId = ProjectionId("projection-with-String", "00")

      val randOffset = UUID.randomUUID().toString
      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, randOffset)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[String](projectionId).futureValue.value
        offset shouldBe randOffset
      }
    }

    "save and retrieve offsets of type akka.persistence.query.Sequence" in {

      val projectionId = ProjectionId("projection-with-akka-seq", "00")

      val seqOffset = Sequence(1L)
      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, seqOffset)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Sequence](projectionId).futureValue.value
        offset shouldBe seqOffset
      }
    }

    "save and retrieve offsets of type akka.persistence.query.TimeBasedUUID" in {

      val projectionId = ProjectionId("projection-with-akka-seq", "00")

      val timeOffset = TimeBasedUUID(UUID.fromString("49225740-2019-11ea-a752-ffae2393b6e4")) //2019-12-16T15:32:36.148Z[UTC]
      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, timeOffset)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[TimeBasedUUID](projectionId).futureValue.value
        offset shouldBe timeOffset
      }
    }

    "update timestamp" in {
      val projectionId = ProjectionId("timestamp", "00")

      val instant0 = clock.instant()
      dbConfig.db.run(offsetStore.saveOffset(projectionId, 15)).futureValue
      val instant1 = selectLastUpdated(projectionId)
      instant1 shouldBe instant0

      val instant2 = clock.tick(java.time.Duration.ofMillis(5))
      dbConfig.db.run(offsetStore.saveOffset(projectionId, 16)).futureValue
      val instant3 = selectLastUpdated(projectionId)
      instant3 shouldBe instant2
    }

    "clear offset" in {
      val projectionId = ProjectionId("projection-clear", "00")

      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, 3L)).futureValue
      }

      withClue("check - read offset") {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(3L)
      }

      withClue("check - clear offset") {
        dbConfig.db.run(offsetStore.clearOffset(projectionId)).futureValue
      }

      withClue("check - read offset") {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe None
      }
    }
  }
}
