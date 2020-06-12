/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mongo

import java.time.Instant
import java.util.UUID

import akka.persistence.query.{ Sequence, TimeBasedUUID }
import akka.projection.ProjectionId
import akka.projection.mongo.internal.{ MongoOffsetStore, MongoSettings }
import akka.projection.testkit.internal.TestClock
import com.typesafe.config.{ Config, ConfigFactory }
import org.mongodb.scala.{ Document, MongoClient }
import org.scalatest.concurrent.{ PatienceConfiguration, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, OptionValues }

import scala.concurrent.Await
import scala.concurrent.duration._

object MongoOffsetStoreSpec {
  def config: Config = ConfigFactory.parseString("""
    akka.projection.mongo = {
      offset-store {
        schema = "test"
        table = "AKKA_PROJECTION_OFFSET_STORE"
      }
    }
    """)
}

class MongoOffsetStoreSpec
    extends MongoSpecBase
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with OptionValues
    with PatienceConfiguration {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(100, Millis))

  private val mongoConfig = MongoOffsetStoreSpec.config.getConfig(MongoSettings.configPath)

  lazy val mongoClient: MongoClient = MongoClient(clientSettings)

  // test clock for testing of the `last_updated` Instant
  private val clock = new TestClock

  private lazy val offsetStore =
    new MongoOffsetStore(mongoClient, MongoSettings(mongoConfig), clock)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // create offset table
    Await.ready(offsetStore.createIfNotExists, 3.seconds)
  }

  override protected def afterAll(): Unit = {
    mongoClient.close()
    super.beforeAll()
  }

  private def selectLastUpdated(projectionId: ProjectionId): Instant = {
    val action = offsetStore.offsetTable
      .find[offsetStore.OffsetRow](
        Document("_id.projectionName" -> projectionId.name, "_id.projectionKey" -> projectionId.key))
      .headOption()
    action.futureValue.get.lastUpdated
  }

  "The OffsetStore" must {

    //implicit val ec: ExecutionContext = ??? //dbConfig.db.executor.executionContext

    "create and update offsets" in {

      val projectionId = ProjectionId("projection-with-long", "00")

      withClue("check - save offset 1L") {
        TransactionCtx.withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, 1L)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 1L
      }

      withClue("check - save offset 2L") {
        TransactionCtx.withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, 2L)).futureValue
      }

      withClue("check - read offset after overwrite") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 2L // yep, saveOffset overwrites previous
      }

    }

    "save and retrieve offsets of type Long" in {

      val projectionId = ProjectionId("projection-with-long", "00")

      withClue("check - save offset") {
        TransactionCtx.withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, 1L)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 1L
      }

    }

    "save and retrieve offsets of type java.lang.Long" in {

      val projectionId = ProjectionId("projection-with-java-long", "00")

      withClue("check - save offset") {
        TransactionCtx
          .withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, java.lang.Long.valueOf(1L)))
          .futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Long](projectionId).futureValue.value
        offset shouldBe 1L
      }
    }

    "save and retrieve offsets of type Int" in {

      val projectionId = ProjectionId("projection-with-int", "00")

      withClue("check - save offset") {
        TransactionCtx.withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, 1)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Int](projectionId).futureValue.value
        offset shouldBe 1
      }

    }

    "save and retrieve offsets of type java.lang.Integer" in {

      val projectionId = ProjectionId("projection-with-java-int", "00")

      withClue("check - save offset") {
        TransactionCtx
          .withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, java.lang.Integer.valueOf(1)))
          .futureValue
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
        TransactionCtx
          .withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, randOffset))
          .futureValue
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
        TransactionCtx
          .withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, seqOffset))
          .futureValue
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
        TransactionCtx
          .withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, timeOffset))
          .futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[TimeBasedUUID](projectionId).futureValue.value
        offset shouldBe timeOffset
      }
    }

    "update timestamp" in {
      val projectionId = ProjectionId("timestamp", "00")

      val instant0 = clock.instant()
      TransactionCtx.withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, 15)).futureValue
      val instant1 = selectLastUpdated(projectionId)
      instant1 shouldBe instant0

      val instant2 = clock.tick(java.time.Duration.ofMillis(5))
      TransactionCtx.withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, 16)).futureValue
      val instant3 = selectLastUpdated(projectionId)
      instant3 shouldBe instant2
    }

    "clear offset" in {
      val projectionId = ProjectionId("projection-clear", "00")

      withClue("check - save offset") {
        TransactionCtx.withSession(mongoClient.startSession())(offsetStore.saveOffset(projectionId, 3L)).futureValue
      }

      withClue("check - read offset") {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(3L)
      }

      withClue("check - clear offset") {
        TransactionCtx.withSession(mongoClient.startSession())(offsetStore.clearOffset(projectionId)).futureValue
      }

      withClue("check - read offset") {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe None
      }
    }
  }
}
