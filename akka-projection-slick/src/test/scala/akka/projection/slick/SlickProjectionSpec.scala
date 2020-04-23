/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.{ Done, NotUsed }
import akka.projection.ProjectionId
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.TestSource
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

  case class Envelope(id: String, offset: Long, message: String)

  def sourceProvider(id: String): SourceProvider[Long, Envelope] = {

    val envelopes =
      List(
        Envelope(id, 1L, "abc"),
        Envelope(id, 2L, "def"),
        Envelope(id, 3L, "ghi"),
        Envelope(id, 4L, "jkl"),
        Envelope(id, 5L, "mno"),
        Envelope(id, 6L, "pqr"))

    TestSourceProvider(Source(envelopes))
  }

  case class TestSourceProvider(src: Source[Envelope, _]) extends SourceProvider[Long, Envelope] {

    override def source(offset: Option[Long]): Source[Envelope, _] = {
      offset match {
        case Some(o) => src.dropWhile(_.offset <= o)
        case _       => src
      }
    }

    override def extractOffset(env: Envelope): Long = env.offset
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

class SlickProjectionSpec extends SlickSpec(SlickProjectionSpec.config) with AnyWordSpecLike with OptionValues {
  import SlickProjectionSpec._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit override val patience: PatienceConfig = PatienceConfig(10.seconds, 500.millis)

  val repository = new TestRepository(dbConfig)

  implicit val dispatcher = testKit.system.executionContext

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    repository.createTable()
  }

  private def genRandomProjectionId() =
    ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

  "A Slick exactly-once projection" must {

    "persist projection and offset in same the same write operation (transactional)" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val slickProjection =
        SlickProjection.exactlyOnce(projectionId, sourceProvider = sourceProvider(entityId), databaseConfig = dbConfig) {
          envelope => repository.concatToText(envelope.id, envelope.message)
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
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val streamFailureMsg = "fail on fourth envelope"
      val slickProjectionFailing =
        SlickProjection.exactlyOnce(projectionId, sourceProvider = sourceProvider(entityId), databaseConfig = dbConfig) {
          envelope =>
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
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig) { envelope => repository.concatToText(envelope.id, envelope.message) }

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
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val streamFailureMsg = "fail on fourth envelope"
      val slickProjectionFailing =
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
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
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig) { envelope => repository.concatToText(envelope.id, envelope.message) }

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
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val slickProjectionFailing =
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
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
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig) { envelope => repository.concatToText(envelope.id, envelope.message) }

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

  "A Slick at-least-once projection" must {

    s"persist projection and offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val slickProjection =
        SlickProjection.atLeastOnce(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          saveOffsetAfterEnvelopes = 1,
          saveOffsetAfterDuration = Duration.Zero) { envelope =>
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

    "restart from previous offset - handler throwing an exception, save after 1" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val streamFailureMsg = "fail on fourth envelope"
      val slickProjectionFailing =
        SlickProjection.atLeastOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          saveOffsetAfterEnvelopes = 1,
          saveOffsetAfterDuration = Duration.Zero) { envelope =>
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
      withClue(s"check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 3L
      }

      // re-run projection without failing function
      val slickProjection =
        SlickProjection.atLeastOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          saveOffsetAfterEnvelopes = 1,
          saveOffsetAfterDuration = Duration.Zero) { envelope =>
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

    "restart from previous offset - handler throwing an exception, save after 2" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val streamFailureMsg = "fail on fourth envelope"
      val slickProjectionFailing =
        SlickProjection.atLeastOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          saveOffsetAfterEnvelopes = 2,
          saveOffsetAfterDuration = 1.minute) { envelope =>
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
      withClue(s"check: last seen offset is 2L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 2L
      }

      // re-run projection without failing function
      val slickProjection =
        SlickProjection.atLeastOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          saveOffsetAfterEnvelopes = 2,
          saveOffsetAfterDuration = 1.minute) { envelope => repository.concatToText(envelope.id, envelope.message) }

      projectionTestKit.run(slickProjection) {
        withClue("checking: all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          // note that 3rd is duplicated
          concatStr.text shouldBe "abc|def|ghi|ghi|jkl|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "save offset after number of elements" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource.probe[Envelope](system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val slickProjection =
        SlickProjection.atLeastOnce[Long, Envelope, H2Profile](
          projectionId = projectionId,
          sourceProvider = TestSourceProvider(source),
          databaseConfig = dbConfig,
          saveOffsetAfterEnvelopes = 10,
          saveOffsetAfterDuration = 1.minute) { envelope => repository.concatToText(envelope.id, envelope.message) }

      val sinkProbe = projectionTestKit.runWithTestSink(slickProjection)
      eventually {
        sourceProbe.get should not be null
      }
      sinkProbe.request(1000)

      (1 to 15).foreach { n => sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n")) }
      eventually {
        dbConfig.db.run(repository.findById(entityId)).futureValue.value.text should include("elem-15")
      }
      offsetStore.readOffset[Long](projectionId).futureValue.value shouldBe 10L

      (16 to 22).foreach { n => sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n")) }
      eventually {
        dbConfig.db.run(repository.findById(entityId)).futureValue.value.text should include("elem-22")
      }
      offsetStore.readOffset[Long](projectionId).futureValue.value shouldBe 20L

      sinkProbe.cancel()
    }

    "save offset after idle duration" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource.probe[Envelope](system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val slickProjection =
        SlickProjection.atLeastOnce[Long, Envelope, H2Profile](
          projectionId = projectionId,
          sourceProvider = TestSourceProvider(source),
          databaseConfig = dbConfig,
          saveOffsetAfterEnvelopes = 10,
          saveOffsetAfterDuration = 2.seconds) { envelope => repository.concatToText(envelope.id, envelope.message) }

      val sinkProbe = projectionTestKit.runWithTestSink(slickProjection)
      eventually {
        sourceProbe.get should not be null
      }
      sinkProbe.request(1000)

      (1 to 15).foreach { n => sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n")) }
      eventually {
        dbConfig.db.run(repository.findById(entityId)).futureValue.value.text should include("elem-15")
      }
      offsetStore.readOffset[Long](projectionId).futureValue.value shouldBe 10L

      (16 to 17).foreach { n => sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n")) }
      eventually {
        offsetStore.readOffset[Long](projectionId).futureValue.value shouldBe 17L
      }
      dbConfig.db.run(repository.findById(entityId)).futureValue.value.text should include("elem-17")

      sinkProbe.cancel()
    }

  }

}
