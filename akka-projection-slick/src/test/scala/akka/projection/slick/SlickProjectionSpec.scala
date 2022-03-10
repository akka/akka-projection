/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.projection.HandlerRecoveryStrategy
import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.TestStatusObserver
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.ProjectionManagement
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.internal.SlickOffsetStore
import akka.projection.slick.internal.SlickSettings
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.projection.testkit.scaladsl.TestSourceProvider
import akka.stream.scaladsl.FlowWithContext
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
       projection.slick {

         profile = "slick.jdbc.H2Profile$"

          # TODO: configure connection pool and slick async executor
          db {
            url = "jdbc:h2:mem:slick-projection-test;DB_CLOSE_DELAY=-1"
            driver = org.h2.Driver
            connectionPool = disabled
            keepAliveConnection = true
          }
       }
      }
      """)

  case class Envelope(id: String, offset: Long, message: String)

  def sourceProvider(
      id: String,
      complete: Boolean = true,
      verifyOffsetFn: Long => OffsetVerification = _ => VerificationSuccess): SourceProvider[Long, Envelope] = {
    val envelopes: Source[Envelope, NotUsed] =
      Source(
        List(
          Envelope(id, 1L, "abc"),
          Envelope(id, 2L, "def"),
          Envelope(id, 3L, "ghi"),
          Envelope(id, 4L, "jkl"),
          Envelope(id, 5L, "mno"),
          Envelope(id, 6L, "pqr")))

    val sp = TestSourceProvider[Long, Envelope](envelopes, _.offset)
      .withOffsetVerification(verifyOffsetFn)
      .withStartSourceFrom((lastProcessedOffset, offset) => offset <= lastProcessedOffset)
    if (complete) sp.withAllowCompletion(true)
    else sp
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
     * Try to insert a row with a null value. This will cause the DB ops to fail
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

class SlickProjectionSpec
    extends ScalaTestWithActorTestKit(SlickProjectionSpec.config)
    with LogCapturing
    with AnyWordSpecLike
    with OptionValues {
  import SlickProjectionSpec._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig(SlickSettings.configPath, config)

  val offsetStore = new SlickOffsetStore(system, dbConfig.db, dbConfig.profile, SlickSettings(system))

  val projectionTestKit = ProjectionTestKit(system)

  override protected def afterAll(): Unit = {
    super.afterAll()
    dbConfig.db.close()
  }
  val repository = new TestRepository(dbConfig)

  private implicit val executionContext: ExecutionContext = system.executionContext

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // create offset table

    val creationFut =
      offsetStore
        .createIfNotExists()
        .flatMap(_ => repository.createTable())

    Await.result(creationFut, 30.seconds)
  }

  private def genRandomProjectionId() = ProjectionId(UUID.randomUUID().toString, "00")

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

    "persist projection and offset in the same write operation (transactional)" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          // build event handler from simple lambda
          handler = () =>
            SlickHandler[Envelope] { envelope =>
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
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            handler = () => bogusEventHandler)
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

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)

      val slickProjection =
        SlickProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndSkip(3, 10.millis))
          .withStatusObserver(statusObserver)

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

      val someTestException = TestException("err")
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectNoMessage()

      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "fail after retrying when using RecoveryStrategy.retryAndFail" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val bogusEventHandler = new ConcatHandlerFail4()

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true)

      val slickProjectionFailing =
        SlickProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(3, 10.millis))
          .withStatusObserver(statusObserver)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      spawn(ProjectionBehavior(slickProjectionFailing))
      eventually {
        val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
        concatStr.text shouldBe "abc|def|ghi"
      }

      statusProbe.expectMessage(TestStatusObserver.Started)

      val someTestException = TestException("err")
      // 1 + 3 => 1 original attempt and 3 retries
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))

      // ultimately, the projection must fail
      statusProbe.expectMessage(TestStatusObserver.Failed)
    }

    "restart from previous offset - fail with DBIOAction.failed" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val bogusEventHandler = new ConcatHandlerFail4()
      val slickProjectionFailing =
        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          () => bogusEventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(slickProjectionFailing) { sinkProbe =>
          sinkProbe.request(1000)
          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
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
      def eventHandler() = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          () => eventHandler())

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
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          () => bogusEventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(slickProjectionFailing) { sinkProbe =>
          sinkProbe.request(1000)
          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
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
      def eventHandler() = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          () => eventHandler())

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
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          () => bogusEventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      projectionTestKit.runWithTestSink(slickProjectionFailing) { sinkProbe =>

        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getClass shouldBe classOf[JdbcSQLIntegrityConstraintViolationException]
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
      def eventHandler() = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          () => eventHandler())

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

    "verify offsets before and after processing an envelope" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val probe = createTestProbe[Any]("verify-and-envelope-msgs")

      val testVerification = (offset: Long) => {
        probe.ref ! offset
        VerificationSuccess
      }

      val slickHandler = SlickHandler[Envelope] { envelope =>
        probe.ref ! envelope
        repository.concatToText(envelope.id, envelope.message)
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = testSourceProvider,
          databaseConfig = dbConfig,
          handler = () => slickHandler)

      val numMessages = 6
      case class VerifyState(verifyCount: Int, envReceived: Boolean)

      projectionTestKit.runWithTestSink(slickProjection) { testSink =>
        for (_ <- 1 to numMessages) testSink.request(1)
        withClue("checking: offset verified once before and once after envelope arrival") {
          val msgs = probe.receiveMessages(numMessages * 3)
          msgs.foldLeft(Map[Long, VerifyState]()) {
            case (acc, msg) =>
              msg match {
                case o: Long =>
                  acc.get(o) match {
                    case Some(state) if !state.envReceived && state.verifyCount >= 1 =>
                      fail(s"Envelope has already been verified at least twice for offset $o")
                    case Some(state) if state.envReceived =>
                      acc + (o -> state.copy(verifyCount = state.verifyCount + 1))
                    case None => acc + (o -> VerifyState(1, false))
                  }
                case e: Envelope =>
                  val o = e.offset
                  acc.get(e.offset) match {
                    case Some(state) if state.envReceived => fail(s"Envelope was already received for offset $o")
                    case Some(state) if state.verifyCount >= 2 =>
                      fail(s"Envelope has already been verified at least twice for offset $o")
                    case None        => fail(s"Envelope has not been verified yet for offset $o")
                    case Some(state) => acc + (o -> state.copy(envReceived = true))
                  }
              }
          }
          probe.expectNoMessage(10.millis)
        }
      }
    }

    "skip record if offset verification fails before processing envelope" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val testVerification = (offset: Long) => {
        if (offset == 3L)
          VerificationFailure("test")
        else
          VerificationSuccess
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = testSourceProvider,
          databaseConfig = dbConfig,
          handler = () =>
            SlickHandler[Envelope] { envelope =>
              repository.concatToText(envelope.id, envelope.message)
            })

      projectionTestKit.run(slickProjection) {
        withClue("checking: all values except skipped were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|jkl|mno|pqr" // `ghi` was skipped
        }
      }
    }

    "skip record if offset verification fails after processing envelope" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val offset3Observed = new AtomicBoolean()

      val testVerification = (offset: Long) => {
        // SkipOffset on the second verification
        if (offset3Observed.get() && offset == 3L)
          VerificationFailure("test")
        else
          VerificationSuccess
      }

      def slickHandler() = SlickHandler[Envelope] { envelope =>
        if (envelope.offset == 3L)
          offset3Observed.set(true)
        repository.concatToText(envelope.id, envelope.message)
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = testSourceProvider,
          databaseConfig = dbConfig,
          handler = () => slickHandler())

      projectionTestKit.run(slickProjection) {
        withClue("checking: all values except skipped were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|jkl|mno|pqr" // `ghi` was skipped
        }
      }
    }
  }

  "A Slick grouped projection" must {

    "persist projection and offset in the same write operation (transactional)" in {

      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val handlerCalled = "called"
      val handlerProbe = testKit.createTestProbe[String]("calls-to-handler")

      val slickProjection =
        SlickProjection
          .groupedWithin(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            // build event handler from simple lambda
            handler = () =>
              SlickHandler[immutable.Seq[Envelope]] { envelopes =>
                handlerProbe.ref ! handlerCalled
                val dbios = envelopes.map(env => repository.concatToText(env.id, env.message))
                DBIOAction.sequence(dbios).map(_ => Done)
              })
          .withGroup(3, 3.seconds)

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

      withClue("check - handler was called only once with grouped envelopes") {
        // handler probe is called twice
        handlerProbe.expectMessage(handlerCalled)
        handlerProbe.expectMessage(handlerCalled)
      }
    }

    "handle grouped async projection and store offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val result = new StringBuffer()

      def handler(): Handler[immutable.Seq[Envelope]] = new Handler[immutable.Seq[Envelope]] {
        override def process(envelopes: immutable.Seq[Envelope]): Future[Done] = {
          Future {
            envelopes.foreach(env => result.append(env.message).append("|"))
          }.map(_ => Done)
        }
      }

      val projection =
        SlickProjection
          .groupedWithinAsync(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            handler = () => handler())
          .withGroup(2, 3.seconds)

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          result.toString shouldBe "abc|def|ghi|jkl|mno|pqr|"
        }
      }
      withClue("check - all offsets were seen") {
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

      def eventHandler() = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection.atLeastOnce(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          () => eventHandler())

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

      def eventHandler() = new ConcatHandlerFail4()
      val slickProjection =
        SlickProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            () => eventHandler())
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

      def eventHandler() = new ConcatHandlerFail4()
      val slickProjection =
        SlickProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            () => eventHandler())
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
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            () => bogusEventHandler)
          .withSaveOffset(1, Duration.Zero)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(slickProjectionFailing) { sinkProbe =>
          sinkProbe.request(1000)
          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        }
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

      def eventHandler() = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection.atLeastOnce(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          () => eventHandler())

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
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            () => bogusEventHandler)
          .withSaveOffset(2, 1.minute)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(slickProjectionFailing) { sinkProbe =>
          sinkProbe.request(1000)
          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        }
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

      def eventHandler() = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection
          .atLeastOnce(
            projectionId = projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            () => eventHandler())
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

      def eventHandler() = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection
          .atLeastOnce[Long, Envelope, H2Profile](
            projectionId = projectionId,
            sourceProvider = TestSourceProvider[Long, Envelope](source, _.offset),
            databaseConfig = dbConfig,
            () => eventHandler())
          .withSaveOffset(10, 1.minute)

      projectionTestKit.runWithTestSink(slickProjection) { sinkProbe =>

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
      }

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

      def eventHandler() = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }
      val slickProjection =
        SlickProjection
          .atLeastOnce[Long, Envelope, H2Profile](
            projectionId = projectionId,
            sourceProvider = TestSourceProvider[Long, Envelope](source, _.offset),
            databaseConfig = dbConfig,
            () => eventHandler())
          .withSaveOffset(10, 2.seconds)

      projectionTestKit.runWithTestSink(slickProjection) { sinkProbe =>

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
      }

    }

    "verify offsets before processing an envelope" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val verifiedProbe: TestProbe[Long] = createTestProbe[Long]()

      val testVerification = (offset: Long) => {
        verifiedProbe.ref ! offset
        VerificationSuccess
      }

      def slickHandler() = SlickHandler[Envelope] { envelope =>
        withClue("checking: offset verified before handler function was run") {
          verifiedProbe.expectMessage(envelope.offset)
        }
        repository.concatToText(envelope.id, envelope.message)
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val slickProjection =
        SlickProjection.atLeastOnce(
          projectionId,
          sourceProvider = testSourceProvider,
          databaseConfig = dbConfig,
          handler = () => slickHandler())

      projectionTestKit.run(slickProjection) {
        withClue("checking: all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }
    }

    "skip record if offset verification fails before processing envelope" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val testVerification = (offset: Long) => {
        if (offset == 3L)
          VerificationFailure("test")
        else
          VerificationSuccess
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val slickProjection =
        SlickProjection.atLeastOnce(
          projectionId,
          sourceProvider = testSourceProvider,
          databaseConfig = dbConfig,
          handler = () =>
            SlickHandler[Envelope] { envelope =>
              repository.concatToText(envelope.id, envelope.message)
            })

      projectionTestKit.run(slickProjection) {
        withClue("checking: all values except skipped were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|jkl|mno|pqr" // `ghi` was skipped
        }
      }
    }

    "handle async projection and store offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val result = new StringBuffer()

      def handler(): Handler[Envelope] = new Handler[Envelope] {
        override def process(envelope: Envelope): Future[Done] = {
          Future {
            result.append(envelope.message).append("|")
          }.map(_ => Done)
        }
      }

      val projection =
        SlickProjection.atLeastOnceAsync(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          databaseConfig = dbConfig,
          handler = () => handler())

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          result.toString shouldBe "abc|def|ghi|jkl|mno|pqr|"
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

  }

  "A Slick flow projection" must {

    "persist projection and offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val flowHandler =
        FlowWithContext[Envelope, ProjectionContext]
          .mapAsync(1) { env =>
            dbConfig.db.run(repository.concatToText(env.id, env.message))
          }

      val projection =
        SlickProjection
          .atLeastOnceFlow(projectionId, sourceProvider(entityId), dbConfig, flowHandler)
          .withSaveOffset(1, 1.minute)

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }
  }

  "SlickProjection lifecycle" must {

    class LifecycleHandler(
        probe: ActorRef[String],
        failOnceOnOffset: AtomicInteger = new AtomicInteger(-1),
        alwaysFailOnOffset: Int = -1)
        extends SlickHandler[Envelope] {

      val createdMessage = "created"
      val startMessage = "start"
      val completedMessage = "completed"
      val failedMessage = "failed"

      // stop message can be 'completed' or 'failed'
      // that allows us to assert that the stopHandler is different execution paths were called in test
      private var stopMessage = completedMessage

      probe ! createdMessage

      override def start(): Future[Done] = {
        // reset stop message to 'completed' on each new start
        stopMessage = completedMessage
        probe ! startMessage
        Future.successful(Done)
      }

      override def stop(): Future[Done] = {
        probe ! stopMessage
        Future.successful(Done)
      }

      override def process(envelope: Envelope): slick.dbio.DBIO[Done] = {
        if (envelope.offset == failOnceOnOffset.get()) {
          failOnceOnOffset.set(-1)
          stopMessage = failedMessage
          slick.dbio.DBIO.failed(TestException(s"Fail $failOnceOnOffset"))
        } else if (envelope.offset == alwaysFailOnOffset) {
          stopMessage = failedMessage
          throw TestException(s"Always Fail $alwaysFailOnOffset")
        } else {
          probe ! envelope.message
          slick.dbio.DBIO.successful(Done)
        }
      }
    }

    "call start and stop of the handler" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref)

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true)

      val projection =
        SlickProjection
          .atLeastOnce[Long, Envelope, H2Profile](
            projectionId = projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            () => handler)
          .withSaveOffset(1, Duration.Zero)
          .withStatusObserver(statusObserver)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

      handlerProbe.expectMessage(handler.createdMessage)

      statusProbe.expectMessage(TestStatusObserver.Started)

      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("abc")
      handlerProbe.expectMessage("def")
      handlerProbe.expectMessage("ghi")
      handlerProbe.expectMessage("jkl")
      handlerProbe.expectMessage("mno")
      handlerProbe.expectMessage("pqr")
      // completed without failure
      handlerProbe.expectMessage(handler.completedMessage)
      handlerProbe.expectNoMessage() // no duplicate stop

      statusProbe.expectMessage(TestStatusObserver.Stopped)
      statusProbe.expectNoMessage()
    }

    "call start and stop of the handler when using TestKit.runWithTestSink" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref)

      val projection =
        SlickProjection
          .atLeastOnce[Long, Envelope, H2Profile](
            projectionId = projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            () => handler)
          .withSaveOffset(1, Duration.Zero)

      // not using ProjectionTestKit because want to test restarts
      projectionTestKit.runWithTestSink(projection) { sinkProbe =>
        // request all 'strings' (abc to pqr)

        handlerProbe.expectMessage(handler.createdMessage)

        // the start happens inside runWithTestSink
        handlerProbe.expectMessage(handler.startMessage)

        // request the elements
        sinkProbe.request(6)
        handlerProbe.expectMessage("abc")
        handlerProbe.expectMessage("def")
        handlerProbe.expectMessage("ghi")
        handlerProbe.expectMessage("jkl")
        handlerProbe.expectMessage("mno")
        handlerProbe.expectMessage("pqr")

        // all elements should have reached the sink
        sinkProbe.expectNextN(6)
      }

      // completed without failure
      handlerProbe.expectMessage(handler.completedMessage)
      handlerProbe.expectNoMessage() // no duplicate stop
    }

    "call start and stop of handler when restarted" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      @volatile var _handler: Option[LifecycleHandler] = None
      val failOnceOnOffset = new AtomicInteger(4)
      val handlerFactory = () => {
        val newHandler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset)
        _handler = Some(newHandler)
        newHandler
      }
      def handler: LifecycleHandler = _handler match {
        case Some(h) => h
        case None =>
          handlerProbe.awaitAssert {
            _handler.get
          }
      }

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val progressProbe = createTestProbe[TestStatusObserver.OffsetProgress[Envelope]]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true, Some(progressProbe.ref))

      val projection =
        SlickProjection
          .atLeastOnce[Long, Envelope, H2Profile](
            projectionId = projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            handlerFactory)
          .withRestartBackoff(1.second, 2.seconds, 0.0)
          .withSaveOffset(1, Duration.Zero)
          .withStatusObserver(statusObserver)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

      handlerProbe.expectMessage(handler.createdMessage)

      statusProbe.expectMessage(TestStatusObserver.Started)

      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("abc")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 1, "abc")))
      handlerProbe.expectMessage("def")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 2, "def")))
      handlerProbe.expectMessage("ghi")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 3, "ghi")))
      // fail 4
      handlerProbe.expectMessage(handler.failedMessage)
      val someTestException = TestException("err")
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))

      // backoff will restart
      statusProbe.expectMessage(TestStatusObserver.Failed)
      statusProbe.expectMessage(TestStatusObserver.Stopped)
      handlerProbe.expectMessage(handler.createdMessage)
      handlerProbe.expectMessage(handler.startMessage)
      statusProbe.expectMessage(TestStatusObserver.Started)
      handlerProbe.expectMessage("jkl")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 4, "jkl")))
      handlerProbe.expectMessage("mno")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 5, "mno")))
      handlerProbe.expectMessage("pqr")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 6, "pqr")))
      // now completed without failure
      handlerProbe.expectMessage(handler.completedMessage)
      handlerProbe.expectNoMessage() // no duplicate stop

      statusProbe.expectMessage(TestStatusObserver.Stopped)
      statusProbe.expectNoMessage()
    }

    "call start and stop of handler when failed but no restart" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val failOnceOnOffset = new AtomicInteger(4)
      val handler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset)

      val projection =
        SlickProjection
          .atLeastOnce[Long, Envelope, H2Profile](
            projectionId = projectionId,
            sourceProvider = sourceProvider(entityId),
            databaseConfig = dbConfig,
            () => handler)
          .withRestartBackoff(1.second, 2.seconds, 0.0, maxRestarts = 0) // no restarts
          .withSaveOffset(1, Duration.Zero)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

      handlerProbe.expectMessage(handler.createdMessage)
      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("abc")
      handlerProbe.expectMessage("def")
      handlerProbe.expectMessage("ghi")
      // fail 4, not restarted
      // completed with failure
      handlerProbe.expectMessage(handler.failedMessage)
      handlerProbe.expectNoMessage() // no duplicate stop
    }

    "be able to stop when retrying" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref, alwaysFailOnOffset = 4)

      val projection =
        SlickProjection
          .atLeastOnce(projectionId, sourceProvider(entityId), dbConfig, () => handler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(100, 100.millis))
          .withSaveOffset(1, Duration.Zero)

      val ref = spawn(ProjectionBehavior(projection))

      handlerProbe.expectMessage(handler.createdMessage)
      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("abc")
      handlerProbe.expectMessage("def")
      handlerProbe.expectMessage("ghi")
      // fail 4

      // let it retry for a while
      Thread.sleep(300)

      ref ! ProjectionBehavior.Stop
      createTestProbe().expectTerminated(ref)
    }
  }

  "SlickProjection management" must {
    "restart from beginning when offset is cleared" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }

      val projection =
        SlickProjection
          .exactlyOnce(
            projectionId,
            sourceProvider(entityId, complete = false),
            databaseConfig = dbConfig,
            () => eventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      // not using ProjectionTestKit because want to test ProjectionManagement
      spawn(ProjectionBehavior(projection))
      eventually {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(6L)
      }

      ProjectionManagement(system).getOffset(projectionId).futureValue shouldBe Some(6L)

      val concatStr1 = dbConfig.db.run(repository.findById(entityId)).futureValue.get
      concatStr1.text shouldBe "abc|def|ghi|jkl|mno|pqr"

      ProjectionManagement(system).clearOffset(projectionId).futureValue shouldBe Done
      eventually {
        val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.get
        concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr|abc|def|ghi|jkl|mno|pqr"
      }
    }

    "restart from updated offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }

      val projection =
        SlickProjection
          .exactlyOnce(
            projectionId,
            sourceProvider(entityId, complete = false),
            databaseConfig = dbConfig,
            () => eventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      // not using ProjectionTestKit because want to test ProjectionManagement
      spawn(ProjectionBehavior(projection))
      eventually {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(6L)
      }

      ProjectionManagement(system).getOffset(projectionId).futureValue shouldBe Some(6L)

      val concatStr1 = dbConfig.db.run(repository.findById(entityId)).futureValue.get
      concatStr1.text shouldBe "abc|def|ghi|jkl|mno|pqr"

      ProjectionManagement(system).updateOffset(projectionId, 3L).futureValue shouldBe Done
      eventually {
        val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.get
        concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr|jkl|mno|pqr"
      }
    }

    "pause projection" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val eventHandler = new SlickHandler[Envelope] {
        override def process(envelope: Envelope): slick.dbio.DBIO[Done] =
          repository.concatToText(envelope.id, envelope.message)
      }

      val projection =
        SlickProjection
          .exactlyOnce(
            projectionId,
            sourceProvider(entityId, complete = false),
            databaseConfig = dbConfig,
            () => eventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      // not using ProjectionTestKit because want to test ProjectionManagement
      spawn(ProjectionBehavior(projection))

      val mgmt = ProjectionManagement(system)

      mgmt.isPaused(projectionId).futureValue shouldBe false

      eventually {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(6L)
      }

      val concatStr1 = dbConfig.db.run(repository.findById(entityId)).futureValue.get
      concatStr1.text shouldBe "abc|def|ghi|jkl|mno|pqr"

      mgmt.pause(projectionId).futureValue shouldBe Done
      mgmt.clearOffset(projectionId).futureValue shouldBe Done

      mgmt.isPaused(projectionId).futureValue shouldBe true

      Thread.sleep(500)
      // not updated because paused
      concatStr1.text shouldBe "abc|def|ghi|jkl|mno|pqr"

      mgmt.resume(projectionId)

      mgmt.isPaused(projectionId).futureValue shouldBe false

      eventually {
        val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.get
        concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr|abc|def|ghi|jkl|mno|pqr"
      }
    }
  }
}
