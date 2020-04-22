/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import java.util.UUID

import akka.persistence.query.{ Sequence, TimeBasedUUID }
import akka.projection.ProjectionId
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, OptionValues }
import slick.basic.DatabaseConfig
import slick.jdbc.H2Profile
import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration._

import akka.projection.slick.internal.SlickOffsetStore

object OffsetStoreSpec {
  def config: Config = ConfigFactory.parseString("""
    akka.projection.slick = {

      profile = "slick.jdbc.H2Profile$"

      # TODO: configure connection pool and slick async executor
      db = {
       url = "jdbc:h2:mem:test1"
       driver = org.h2.Driver
       connectionPool = disabled
       keepAliveConnection = true
      }
    }
    """)
}
class OffsetStoreSpec extends AnyWordSpecLike with Matchers with ScalaFutures with BeforeAndAfterAll with OptionValues {

  val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig("akka.projection.slick", OffsetStoreSpec.config)

  val offsetStore = new SlickOffsetStore(dbConfig.db, dbConfig.profile)

  override protected def beforeAll(): Unit = {
    // create offset table
    Await.ready(offsetStore.createIfNotExists, 3.seconds)
  }

  override protected def afterAll(): Unit = {
    dbConfig.db.close()
  }

  "The OffsetStore" must {

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
  }
}
