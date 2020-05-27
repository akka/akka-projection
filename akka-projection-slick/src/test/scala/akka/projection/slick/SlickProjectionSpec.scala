/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.actor.testkit.typed.TestException
import akka.projection.OffsetVerification
import akka.projection.Success
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSource
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException
import org.scalatest.OptionValues
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import slick.basic.DatabaseConfig
import slick.dbio.DBIOAction
import slick.jdbc.H2Profile

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

  def sourceProvider(systemProvider: ClassicActorSystemProvider, id: String): SourceProvider[Long, Envelope] = {

    val envelopes =
      List(
        Envelope(id, 1L, "abc"),
        Envelope(id, 2L, "def"),
        Envelope(id, 3L, "ghi"),
        Envelope(id, 4L, "jkl"),
        Envelope(id, 5L, "mno"),
        Envelope(id, 6L, "pqr"))

    TestSourceProvider(systemProvider, Source(envelopes))
  }

  case class TestSourceProvider(systemProvider: ClassicActorSystemProvider, src: Source[Envelope, _])
      extends SourceProvider[Long, Envelope] {
    implicit val dispatcher: ExecutionContext = systemProvider.classicSystem.dispatcher
    override def source(offset: () => Future[Option[Long]]): Future[Source[Envelope, _]] =
      offset().map {
        case Some(o) => src.dropWhile(_.offset <= o)
        case _       => src
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

  val repository = new TestRepository(dbConfig)

  implicit val actorSystem = testKit.system
  implicit val dispatcher = testKit.system.executionContext

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    repository.createTable()
  }

  private def genRandomProjectionId() =
    ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

  @tailrec private def eventuallyExpectError(sinkProbe: TestSubscriber.Probe[_]): Throwable = {
    sinkProbe.expectNextOrError() match {
      case Right(_)  => eventuallyExpectError(sinkProbe)
      case Left(exc) => exc
    }
  }

  private val concatHandlerFail4Msg = "fail on fourth envelope"

  class ConcatHandlerFail4() extends SlickHandler[Envelope] {
    private val _attempts = new AtomicInteger()
    def attempts: Int = _attempts.get

    override def process(envelope: Envelope): slick.dbio.DBIO[Done] = {
      if (envelope.offset == 4L) {
        _attempts.incrementAndGet()
        DBIOAction.failed(TestException(concatHandlerFail4Msg + s" after $attempts attempts"))
      } else {
        repository.concatToText(envelope.id, envelope.message)
      }
    }
  }

  "A Slick exactly-once projection" must {

    "persist projection and offset in same the same write operation (transactional)" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(system, entityId),
          databaseConfig = dbConfig,
          // build event handler from simple lambda
          handler = SlickHandler[Envelope] { envelope =>
            repository.concatToText(envelope.id, envelope.message)
          })

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

    "skip failing events when using RecoveryStrategy.skip" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val bogusEventHandler = new ConcatHandlerFail4()

      val slickProjection =
        SlickProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(system, entityId),
            databaseConfig = dbConfig,
            handler = bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      projectionTestKit.run(slickProjection) {
        withClue("check - not all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|mno|pqr"
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "skip failing events after retrying when using RecoveryStrategy.retryAndSkip" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val bogusEventHandler = new ConcatHandlerFail4()

      val slickProjection =
        SlickProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(system, entityId),
            databaseConfig = dbConfig,
            handler = bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndSkip(3, 10.millis))

      projectionTestKit.run(slickProjection) {
        withClue("check - not all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|mno|pqr"
        }
      }

      withClue("check - event handler did failed 4 times") {
        // 1 + 3 => 1 original attempt and 3 retries
        bogusEventHandler.attempts shouldBe 1 + 3
      }

      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "fail after retrying when using RecoveryStrategy.retryAndFail" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val bogusEventHandler = new ConcatHandlerFail4()

      val slickProjectionFailing =
        SlickProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(system, entityId),
            databaseConfig = dbConfig,
            bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(3, 10.millis))

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        val sinkProbe = projectionTestKit.runWithTestSink(slickProjectionFailing)
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
      }
      withClue("check: projection is consumed up to third") {
        val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
        concatStr.text shouldBe "abc|def|ghi"
      }

      withClue("check - event handler did failed 4 times") {
        // 1 + 3 => 1 original attempt and 3 retries
        bogusEventHandler.attempts shouldBe 1 + 3
      }

      withClue("check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 3L
      }

    }

    "restart from previous offset - fail with DBIOAction.failed" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val bogusEventHandler = new ConcatHandlerFail4()
      val slickProjectionFailing =
        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(system, entityId),
          databaseConfig = dbConfig,
          bogusEventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        val sinkProbe = projectionTestKit.runWithTestSink(slickProjectionFailing)
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
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
      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(system, entityId),
          databaseConfig = dbConfig,
          eventHandler)

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

      val bogusEventHandler = new ConcatHandlerFail4()
      val slickProjectionFailing =
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(system, entityId),
          databaseConfig = dbConfig,
          bogusEventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        val sinkProbe = projectionTestKit.runWithTestSink(slickProjectionFailing)
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
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
      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(system, entityId),
          databaseConfig = dbConfig,
          eventHandler)

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

      val bogusEventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          if (envelope.offset == 4L) repository.updateWithNullValue(envelope.id)
          else repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjectionFailing =
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(system, entityId),
          databaseConfig = dbConfig,
          bogusEventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val sinkProbe = projectionTestKit.runWithTestSink(slickProjectionFailing)
      sinkProbe.request(1000)
      eventuallyExpectError(sinkProbe).getClass shouldBe classOf[JdbcSQLIntegrityConstraintViolationException]

      withClue("check: projection is consumed up to third") {
        val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 3L
      }

      // re-run projection without failing function
      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(system, entityId),
          databaseConfig = dbConfig,
          eventHandler)

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

    "persist projection and offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection.atLeastOnce(
          projectionId,
          sourceProvider = sourceProvider(system, entityId),
          databaseConfig = dbConfig,
          eventHandler)

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

    "skip failing events when using RecoveryStrategy.skip, save after 1" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val eventHandler = new ConcatHandlerFail4()
      val slickProjection =
        SlickProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = sourceProvider(system, entityId),
            databaseConfig = dbConfig,
            eventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      projectionTestKit.run(slickProjection) {
        withClue("check - all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|mno|pqr"
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "skip failing events when using RecoveryStrategy.skip, save after 2" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val eventHandler = new ConcatHandlerFail4()
      val slickProjection =
        SlickProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = sourceProvider(system, entityId),
            databaseConfig = dbConfig,
            eventHandler)
          .withSaveOffset(2, 1.minute)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      projectionTestKit.run(slickProjection) {
        withClue("check - all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|mno|pqr"
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

      val bogusEventHandler = new ConcatHandlerFail4()
      val slickProjectionFailing =
        SlickProjection
          .atLeastOnce(
            projectionId = projectionId,
            sourceProvider = sourceProvider(system, entityId),
            databaseConfig = dbConfig,
            bogusEventHandler)
          .withSaveOffset(1, Duration.Zero)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        val sinkProbe = projectionTestKit.runWithTestSink(slickProjectionFailing)
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
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

      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection.atLeastOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(system, entityId),
          databaseConfig = dbConfig,
          eventHandler)

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

      val bogusEventHandler = new ConcatHandlerFail4()
      val slickProjectionFailing =
        SlickProjection
          .atLeastOnce(
            projectionId = projectionId,
            sourceProvider = sourceProvider(system, entityId),
            databaseConfig = dbConfig,
            bogusEventHandler)
          .withSaveOffset(2, 1.minute)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        val sinkProbe = projectionTestKit.runWithTestSink(slickProjectionFailing)
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
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

      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection
          .atLeastOnce(
            projectionId = projectionId,
            sourceProvider = sourceProvider(system, entityId),
            databaseConfig = dbConfig,
            eventHandler)
          .withSaveOffset(2, 1.minute)

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

      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection
          .atLeastOnce[Long, Envelope, H2Profile](
            projectionId = projectionId,
            sourceProvider = TestSourceProvider(system, source),
            databaseConfig = dbConfig,
            eventHandler)
          .withSaveOffset(10, 1.minute)

      val sinkProbe = projectionTestKit.runWithTestSink(slickProjection)
      eventually {
        sourceProbe.get should not be null
      }
      sinkProbe.request(1000)

      (1 to 15).foreach { n =>
        sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
      }
      eventually {
        dbConfig.db.run(repository.findById(entityId)).futureValue.value.text should include("elem-15")
      }
      offsetStore.readOffset[Long](projectionId).futureValue.value shouldBe 10L

      (16 to 22).foreach { n =>
        sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
      }
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

      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection
          .atLeastOnce[Long, Envelope, H2Profile](
            projectionId = projectionId,
            sourceProvider = TestSourceProvider(system, source),
            databaseConfig = dbConfig,
            eventHandler)
          .withSaveOffset(10, 2.seconds)
      val sinkProbe = projectionTestKit.runWithTestSink(slickProjection)
      eventually {
        sourceProbe.get should not be null
      }
      sinkProbe.request(1000)

      (1 to 15).foreach { n =>
        sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
      }
      eventually {
        dbConfig.db.run(repository.findById(entityId)).futureValue.value.text should include("elem-15")
      }
      offsetStore.readOffset[Long](projectionId).futureValue.value shouldBe 10L

      (16 to 17).foreach { n =>
        sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
      }
      eventually {
        offsetStore.readOffset[Long](projectionId).futureValue.value shouldBe 17L
      }
      dbConfig.db.run(repository.findById(entityId)).futureValue.value.text should include("elem-17")

      sinkProbe.cancel()
    }

  }

}
