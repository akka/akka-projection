/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import java.util.UUID

import akka.projection.ProjectionId
import akka.stream.scaladsl.Source
import akka.{ Done, NotUsed }
import com.typesafe.config.{ Config, ConfigFactory }
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException
import org.scalatest.OptionValues
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.{ Logger, LoggerFactory }
import slick.basic.DatabaseConfig
import slick.dbio.DBIOAction
import slick.jdbc.H2Profile

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

object SlickProjectionSpec {
  def config: Config = ConfigFactory.parseString("""
      akka {
       loglevel = "DEBUG"
       projection.slick = {

         profile = "slick.jdbc.H2Profile$"

         # TODO: configure connection pool and slick async executor
         db = {
          url = "jdbc:h2:mem:test1"
          driver = org.h2.Driver
          connectionPool = disabled
          keepAliveConnection = true
         }
       }
      }
      """)
}

class SlickProjectionSpec extends SlickSpec(SlickProjectionSpec.config) with AnyWordSpecLike with OptionValues {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit override val patience: PatienceConfig = PatienceConfig(10.seconds, 500.millis)

  val repository = new TestRepository(dbConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    repository.createTable()
  }

  private def genRandomProjectionId() =
    ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

  "A Slick projection" must {

    "persist projection and offset in same the same write operation (transactional)" in {

      implicit val dispatcher = testKit.system.executionContext

      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val slickProjection =
        SlickProjection.transactional(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          offsetExtractor = offsetExtractor,
          databaseConfig = dbConfig) { envelope =>
          repository.concatToText(envelope.id, envelope.message)
        }

      projectionTestKit.run(slickProjection) {
        withClue("check - all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "restart from previous offset - fail with DBIOAction.failed" in {

      implicit val dispatcher = testKit.system.executionContext

      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val streamFailureMsg = "fail on fourth element"
      val slickProjectionFailing =
        SlickProjection.transactional(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          offsetExtractor = offsetExtractor,
          databaseConfig = dbConfig) { envelope =>
          if (envelope.offset == 4L) DBIOAction.failed(new RuntimeException(streamFailureMsg))
          else repository.concatToText(envelope.id, envelope.message)
        }

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val streamFailure =
        intercept[RuntimeException] {
          projectionTestKit.run(slickProjectionFailing) {
            fail("fail assertFunc, we want to capture the stream failure")
          }
        }

      withClue("check: projection failed with stream failure") {
        streamFailure.getMessage shouldBe streamFailureMsg
      }
      withClue("check: projection is consumed up to third") {
        val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 3L
      }

      // re-run projection without failing function
      val slickProjection =
        SlickProjection.transactional(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          offsetExtractor = offsetExtractor,
          databaseConfig = dbConfig) { envelope =>
          repository.concatToText(envelope.id, envelope.message)
        }

      projectionTestKit.run(slickProjection) {
        withClue("checking: all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "restart from previous offset - fail with throwing an exception" in {

      implicit val dispatcher = testKit.system.executionContext

      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val streamFailureMsg = "fail on fourth element"
      val slickProjectionFailing =
        SlickProjection.transactional(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          offsetExtractor = offsetExtractor,
          databaseConfig = dbConfig) { envelope =>
          if (envelope.offset == 4L) throw new RuntimeException(streamFailureMsg)
          else repository.concatToText(envelope.id, envelope.message)
        }

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val streamFailure =
        intercept[RuntimeException] {
          projectionTestKit.run(slickProjectionFailing) {
            fail("fail assertFunc, we want to capture the stream failure")
          }
        }

      withClue("check: projection failed with stream failure") {
        streamFailure.getMessage shouldBe streamFailureMsg
      }
      withClue("check: projection is consumed up to third") {
        val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 3L
      }

      // re-run projection without failing function
      val slickProjection =
        SlickProjection.transactional(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          offsetExtractor = offsetExtractor,
          databaseConfig = dbConfig) { envelope =>
          repository.concatToText(envelope.id, envelope.message)
        }

      projectionTestKit.run(slickProjection) {
        withClue("checking: all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "restart from previous offset - fail with bad insert on user code" in {

      implicit val dispatcher = testKit.system.executionContext

      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val slickProjectionFailing =
        SlickProjection.transactional(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          offsetExtractor = offsetExtractor,
          databaseConfig = dbConfig) { envelope =>
          if (envelope.offset == 4L) repository.updateWithNullValue(envelope.id)
          else repository.concatToText(envelope.id, envelope.message)
        }

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      intercept[JdbcSQLIntegrityConstraintViolationException] {
        projectionTestKit.run(slickProjectionFailing) {
          fail("fail assertFunc, we want to capture the stream failure")
        }
      }

      withClue("check: projection is consumed up to third") {
        val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 3L
      }

      // re-run projection without failing function
      val slickProjection =
        SlickProjection.transactional(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          offsetExtractor = offsetExtractor,
          databaseConfig = dbConfig) { envelope =>
          repository.concatToText(envelope.id, envelope.message)
        }

      projectionTestKit.run(slickProjection) {
        withClue("checking: all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }
  }

  case class Envelope(id: String, offset: Long, message: String)

  def offsetExtractor(env: Envelope): Long = env.offset

  def sourceProvider(id: String)(offset: Option[Long]): Source[Envelope, NotUsed] = {
    val elements =
      List(
        Envelope(id, 1L, "abc"),
        Envelope(id, 2L, "def"),
        Envelope(id, 3L, "ghi"),
        Envelope(id, 4L, "jkl"),
        Envelope(id, 5L, "mno"),
        Envelope(id, 6L, "pqr"))

    val src = Source(elements)

    offset match {
      case Some(o) => src.dropWhile(_.offset <= o)
      case _       => src
    }
  }

  // test model is as simple as a text that gets other string concatenated to it
  case class ConcatStr(id: String, text: String) {
    def concat(newMsg: String) = copy(text = text + "|" + newMsg)
  }

  class TestRepository(val dbConfig: DatabaseConfig[H2Profile]) {

    import dbConfig.profile.api._

    private class ConcatStrTable(tag: Tag) extends Table[ConcatStr](tag, "TEST_MODEL") {
      def id = column[String]("ID", O.PrimaryKey)

      def concatenated = column[String]("CONCATENATED")

      def * = (id, concatenated).mapTo[ConcatStr]
    }

    def concatToText(id: String, payload: String)(implicit ec: ExecutionContext) = {
      for {
        concatStr <- findById(id).map {
          case Some(concatStr) => concatStr.concat(payload)
          case _               => ConcatStr(id, payload)
        }
        _ <- concatStrTable.insertOrUpdate(concatStr)
      } yield Done
    }

    /**
     * Try to insert a row with a null value. This will code the DB ops to fail
     */
    def updateWithNullValue(id: String)(implicit ec: ExecutionContext) = {
      concatStrTable.insertOrUpdate(ConcatStr(id, null)).map(_ => Done)
    }

    def findById(id: String): DBIO[Option[ConcatStr]] =
      concatStrTable.filter(_.id === id).result.headOption

    private val concatStrTable = TableQuery[ConcatStrTable]

    def readValue(id: String): Future[String] = {
      // map using Slick's own EC
      implicit val ec = dbConfig.db.executor.executionContext
      val action = findById(id).map {
        case Some(concatStr) => concatStr.text
        case _               => "N/A"
      }
      dbConfig.db.run(action)
    }

    def createTable(): Future[Unit] =
      dbConfig.db.run(concatStrTable.schema.createIfNotExists)
  }

}
