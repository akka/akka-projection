/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
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
import akka.actor.typed.ActorSystem
import akka.japi.function
import akka.projection.HandlerRecoveryStrategy
import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.TestStatusObserver
import akka.projection.jdbc.internal.JdbcOffsetStore
import akka.projection.jdbc.internal.JdbcSessionUtil
import akka.projection.jdbc.internal.JdbcSettings
import akka.projection.jdbc.scaladsl.JdbcHandler
import akka.projection.jdbc.scaladsl.JdbcProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.ProjectionManagement
import akka.projection.scaladsl.SourceProvider
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

object JdbcProjectionSpec {
  val config: Config = ConfigFactory.parseString(s"""
    akka {
      loglevel = "DEBUG"
      projection.jdbc {
        dialect = "h2-dialect"
        offset-store {
          schema = ""
          table = "akka_projection_offset_store"
        }
        
        blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 5
        debug.verbose-offset-store-logging = true
      }
    }
    """)

  class PureJdbcSession extends JdbcSession {

    lazy val conn = {
      Class.forName("org.h2.Driver")
      val c = DriverManager.getConnection("jdbc:h2:mem:jdbc-projection-test;DB_CLOSE_DELAY=-1")
      c.setAutoCommit(false)
      c
    }

    override def withConnection[Result](func: function.Function[Connection, Result]): Result =
      func(conn)

    override def commit(): Unit = conn.commit()
    override def rollback(): Unit = conn.rollback()
    override def close(): Unit = conn.close()
  }

  val jdbcSessionFactory = () => new PureJdbcSession

  case class Envelope(id: String, offset: Long, message: String)

  def sourceProvider(
      id: String,
      complete: Boolean = true,
      verifyOffsetFn: Long => OffsetVerification = _ => VerificationSuccess): SourceProvider[Long, Envelope] = {
    val envelopes: Source[Envelope, NotUsed] =
      Source(
        List(
          Envelope(id, 1L, "e1"),
          Envelope(id, 2L, "e2"),
          Envelope(id, 3L, "e3"),
          Envelope(id, 4L, "e4"),
          Envelope(id, 5L, "e5"),
          Envelope(id, 6L, "e6")))

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

  case class TestRepository(conn: Connection) {

    private val logger = LoggerFactory.getLogger(this.getClass)

    private val table = "TEST_MODEL"

    def concatToText(id: String, payload: String): Done = {
      val savedStrOpt = findById(id)

      val concatStr =
        savedStrOpt
          .map { _.concat(payload) }
          .getOrElse(ConcatStr(id, payload))

      insertOrUpdate(concatStr)
    }

    def updateWithNullValue(id: String): Done = {
      insertOrUpdate(ConcatStr(id, null))
    }

    private def insertOrUpdate(concatStr: ConcatStr): Done = {
      logger.debug(
        "TestRepository.insertOrUpdate: [{}], using connection id [{}]",
        concatStr,
        System.identityHashCode(conn))
      val stmtStr = s"""MERGE INTO "$table" ("ID","CONCATENATED")  VALUES (?,?)"""
      JdbcSessionUtil.tryWithResource(conn.prepareStatement(stmtStr)) { stmt =>
        stmt.setString(1, concatStr.id)
        stmt.setString(2, concatStr.text)
        stmt.executeUpdate()
        Done
      }
    }

    def findById(id: String): Option[ConcatStr] = {

      val stmtStr = s"SELECT * FROM $table WHERE ID = ?"

      JdbcSessionUtil.tryWithResource(conn.prepareStatement(stmtStr)) { stmt =>
        logger.debug("TestRepository.findById: [{}], using connection id [{}]", id, System.identityHashCode(conn))
        stmt.setString(1, id)
        val resultSet = stmt.executeQuery()
        if (resultSet.first()) {
          Some(ConcatStr(resultSet.getString("ID"), resultSet.getString("CONCATENATED")))
        } else None
      }
    }

    def createTable(): Done = {

      val createTableStatement = s"""
       create table if not exists "$table" (
        "ID" CHAR(255) NOT NULL,
        "CONCATENATED" CHARACTER VARYING(255) NOT NULL
       );
       """

      val alterTableStatement =
        s"""alter table "$table" add constraint "PK_ID" primary key("ID");"""

      JdbcSessionUtil.tryWithResource(conn.createStatement()) { stmt =>
        logger.debug("TestRepository.createTable using connection id [{}]", System.identityHashCode(conn))
        stmt.execute(createTableStatement)
        stmt.execute(alterTableStatement)
        Done
      }

    }
  }

}

class JdbcProjectionSpec
    extends ScalaTestWithActorTestKit(JdbcProjectionSpec.config)
    with AnyWordSpecLike
    with LogCapturing
    with OptionValues {

  import JdbcProjectionSpec._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val actorSystem: ActorSystem[Nothing] = testKit.system
  implicit val executionContext: ExecutionContext = testKit.system.executionContext

  val jdbcSettings = JdbcSettings(testKit.system)
  implicit val offsetStore: JdbcOffsetStore[PureJdbcSession] =
    new JdbcOffsetStore(system, jdbcSettings, jdbcSessionFactory)

  val projectionTestKit = ProjectionTestKit(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // create offset table
    val creationFut = offsetStore
      .createIfNotExists()
      .flatMap(_ =>
        JdbcSessionUtil.withConnection(jdbcSessionFactory) { conn =>
          TestRepository(conn).createTable()
        })
    Await.result(creationFut, 3.seconds)
  }

  private def genRandomProjectionId() = ProjectionId(UUID.randomUUID().toString, "00")

  private def offsetShouldBe(
      expected: Long)(implicit offsetStore: JdbcOffsetStore[PureJdbcSession], projectionId: ProjectionId) = {
    offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(expected)
  }

  private def offsetShouldBeEmpty()(
      implicit offsetStore: JdbcOffsetStore[PureJdbcSession],
      projectionId: ProjectionId) = {
    offsetStore.readOffset[Long](projectionId).futureValue shouldBe empty
  }

  private def projectedValueShouldBe(expected: String)(implicit entityId: String) = {
    val opt = withRepo(_.findById(entityId)).futureValue.map(_.text)
    opt shouldBe Some(expected)
  }

  private def projectedValueShouldInclude(expected: String)(implicit entityId: String) = {
    withClue(s"checking projected value contains [$expected]: ") {
      val text = withRepo(_.findById(entityId)).futureValue.value.text
      text should include(expected)
    }
  }

  private def projectedValueShould(actual: String => Boolean)(implicit entityId: String) = {
    withClue(s"checking projected value fulfil predicate: ") {
      val text = withRepo(_.findById(entityId)).futureValue.value.text
      actual(text)
    }
  }

  private def projectedValueShouldIncludeNTimes(expected: String, nTimes: Int)(implicit entityId: String) = {
    withClue(s"checking projected value contains [$expected] $nTimes times: ") {
      val text = withRepo(_.findById(entityId)).futureValue.value.text
      text.split("\\|").count(_ == expected) shouldBe nTimes
    }
  }

  // TODO: extract this to some utility
  @tailrec private def eventuallyExpectError(sinkProbe: TestSubscriber.Probe[_]): Throwable = {
    sinkProbe.expectNextOrError() match {
      case Right(_)  => eventuallyExpectError(sinkProbe)
      case Left(exc) => exc
    }
  }

  private val concatHandlerFail4Msg = "fail on fourth envelope"

  private def withRepo[R](block: TestRepository => R): Future[R] = {
    JdbcSessionUtil.withConnection(jdbcSessionFactory) { conn =>
      block(TestRepository(conn))
    }
  }

  class ConcatHandler(failPredicate: Long => Boolean = _ => false) extends JdbcHandler[Envelope, PureJdbcSession] {

    private val _attempts = new AtomicInteger()
    def attempts: Int = _attempts.get

    override def process(session: PureJdbcSession, envelope: Envelope): Unit = {
      if (failPredicate(envelope.offset)) {
        _attempts.incrementAndGet()
        throw TestException(concatHandlerFail4Msg + s" after $attempts attempts")
      } else {
        session.withConnection { conn =>
          logger.debug(s"handling $envelope using [${System.identityHashCode(conn)}])")
          TestRepository(conn).concatToText(envelope.id, envelope.message)
        }
      }
    }
  }

  "A JDBC exactly-once projection" must {

    "persist projection and offset in the same write operation (transactional)" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val projection =
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          jdbcSessionFactory,
          handler = () => new ConcatHandler)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(6L)
    }

    "skip failing events when using RecoveryStrategy.skip" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val bogusEventHandler = new ConcatHandler(_ == 4)

      val projectionFailing =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        projectedValueShouldBe("e1|e2|e3|e5|e6")
      }
      offsetShouldBe(6L)
    }

    "store offset for failing events when using RecoveryStrategy.skip" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val bogusEventHandler = new ConcatHandler(_ == 6)

      val projectionFailing =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        projectedValueShouldBe("e1|e2|e3|e4|e5")
      }
      offsetShouldBe(6L)
    }

    "skip failing events after retrying when using RecoveryStrategy.retryAndSkip" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val bogusEventHandler = new ConcatHandler(_ == 4)

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val progressProbe = createTestProbe[TestStatusObserver.OffsetProgress[Envelope]]()
      val statusObserver =
        new TestStatusObserver[Envelope](statusProbe.ref, offsetProgressProbe = Some(progressProbe.ref))

      val projectionFailing =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndSkip(3, 10.millis))
          .withStatusObserver(statusObserver)

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        projectedValueShouldBe("e1|e2|e3|e5|e6")
      }

      // 1 + 3 => 1 original attempt and 3 retries
      bogusEventHandler.attempts shouldBe 1 + 3

      val someTestException = TestException("err")
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "e4"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "e4"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "e4"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "e4"), someTestException))
      statusProbe.expectNoMessage()
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 1, "e1")))
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 2, "e2")))
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 3, "e3")))
      // Offset 4 is stored even though it failed and was skipped
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 4, "e4")))
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 5, "e5")))
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 6, "e6")))

      offsetShouldBe(6L)
    }

    "fail after retrying when using RecoveryStrategy.retryAndFail" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val bogusEventHandler = new ConcatHandler(_ == 4)

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true)

      val projectionFailing =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(3, 10.millis))
          .withStatusObserver(statusObserver)

      offsetShouldBeEmpty()
      spawn(ProjectionBehavior(projectionFailing))

      eventually {
        projectedValueShouldBe("e1|e2|e3")
      }

      statusProbe.expectMessage(TestStatusObserver.Started)

      val someTestException = TestException("err")
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "e4"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "e4"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "e4"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "e4"), someTestException))

      // ultimately, the projection must fail
      statusProbe.expectMessage(TestStatusObserver.Failed)
      offsetShouldBe(3L)
    }

    "restart from previous offset - fail with throwing an exception" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      def exactlyOnceProjection(failWhenOffset: Long => Boolean = _ => false) = {
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          jdbcSessionFactory,
          handler = () => new ConcatHandler(failWhenOffset))
      }

      offsetShouldBeEmpty()
      projectionTestKit.runWithTestSink(exactlyOnceProjection(_ == 4)) { sinkProbe =>
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
      }
      projectedValueShouldBe("e1|e2|e3")
      offsetShouldBe(3L)

      // re-run projection without failing function
      projectionTestKit.run(exactlyOnceProjection()) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(6L)
    }

    "restart from previous offset - fail with bad insert on user code" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val bogusEventHandler = new JdbcHandler[Envelope, PureJdbcSession] {
        override def process(session: PureJdbcSession, envelope: Envelope): Unit = {
          session.withConnection { conn =>
            val repo = TestRepository(conn)
            if (envelope.offset == 4L) repo.updateWithNullValue(envelope.id)
            else repo.concatToText(envelope.id, envelope.message)
          }
        }
      }

      def exactlyOnceProjection(handler: () => JdbcHandler[Envelope, PureJdbcSession]) =
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          jdbcSessionFactory,
          handler = handler)

      offsetShouldBeEmpty()
      projectionTestKit.runWithTestSink(exactlyOnceProjection(() => bogusEventHandler)) { sinkProbe =>
        sinkProbe.request(4)
        eventuallyExpectError(sinkProbe).getClass shouldBe classOf[JdbcSQLIntegrityConstraintViolationException]
      }
      projectedValueShouldBe("e1|e2|e3")
      offsetShouldBe(3L)

      // re-run projection without failing function
      projectionTestKit.run(exactlyOnceProjection(() => new ConcatHandler())) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(6L)
    }

    "verify offsets before and after processing an envelope" in {

      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      case class ProbeMessage(str: String, offset: Long)
      val verificationProbe = testKit.createTestProbe[ProbeMessage]("verification")
      val processProbe = testKit.createTestProbe[ProbeMessage]("processing")

      val testVerification = (offset: Long) => {
        verificationProbe.ref ! ProbeMessage("verification", offset)
        VerificationSuccess
      }

      val handler = new JdbcHandler[Envelope, PureJdbcSession] {
        override def process(session: PureJdbcSession, envelope: Envelope): Unit = {

          verificationProbe.receiveMessage().offset shouldEqual envelope.offset
          processProbe.ref ! ProbeMessage("process", envelope.offset)

          session.withConnection { conn =>
            TestRepository(conn).concatToText(envelope.id, envelope.message)
          }
        }
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = testSourceProvider,
          jdbcSessionFactory,
          handler = () => handler)

      projectionTestKit.runWithTestSink(projection) { testSink =>
        for (i <- 1 to 6) {
          testSink.request(1)
          processProbe.receiveMessage().offset shouldBe i
        }
      }

    }

    "skip record if offset verification fails before processing envelope" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val testVerification = (offset: Long) => {
        if (offset == 3L)
          VerificationFailure("test")
        else
          VerificationSuccess
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = testSourceProvider,
          jdbcSessionFactory,
          handler = () => new ConcatHandler())

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e4|e5|e6") // `e3` was skipped
      }
    }

    "skip record if offset verification fails after processing envelope" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val testVerification = (offset: Long) => {
        if (offset == 3L)
          VerificationFailure("test")
        else
          VerificationSuccess
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = testSourceProvider,
          jdbcSessionFactory,
          handler = () => new ConcatHandler())

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e4|e5|e6") // `e3` was skipped
      }
    }
  }

  "A JDBC grouped projection" must {
    "persist projection and offset in the same write operation (transactional)" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val handlerCalled = "called"
      val handlerProbe = testKit.createTestProbe[String]("calls-to-handler")

      val projection =
        JdbcProjection
          .groupedWithin(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () =>
              JdbcHandler[PureJdbcSession, immutable.Seq[Envelope]] { (sess, envelopes) =>
                handlerProbe.ref ! handlerCalled
                sess.withConnection { conn =>
                  envelopes.foreach { envelope =>
                    TestRepository(conn).concatToText(envelope.id, envelope.message)
                  }
                }
              })
          .withGroup(3, 3.seconds)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(6L)

      // handler probe is called twice
      handlerProbe.expectMessage(handlerCalled)
      handlerProbe.expectMessage(handlerCalled)
    }

    "handle grouped async projection and store offset" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val result = new StringBuffer()

      def handler(): Handler[immutable.Seq[Envelope]] = new Handler[immutable.Seq[Envelope]] {
        override def process(envelopes: immutable.Seq[Envelope]): Future[Done] = {
          Future {
            envelopes.foreach(env => result.append(env.message).append("|"))
          }.map(_ => Done)
        }
      }

      val projection =
        JdbcProjection
          .groupedWithinAsync(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => handler())
          .withGroup(2, 3.seconds)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        result.toString shouldBe "e1|e2|e3|e4|e5|e6|"
      }
      offsetShouldBe(6L)
    }
  }

  "A JDBC at-least-once projection" must {

    "persist projection and offset" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val projection =
        JdbcProjection.atLeastOnce(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          jdbcSessionFactory,
          handler = () => new ConcatHandler)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(6L)
    }

    "skip failing events when using RecoveryStrategy.skip, save after 1" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val projection =
        JdbcProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => new ConcatHandler(_ == 4))
          .withSaveOffset(1, 1.minute)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e5|e6")
      }
      offsetShouldBe(6L)
    }

    "skip failing events when using RecoveryStrategy.skip, save after 2" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val projection =
        JdbcProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => new ConcatHandler(_ == 4))
          .withSaveOffset(2, 1.minute)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e5|e6")
      }
      offsetShouldBe(6L)
    }

    "restart from previous offset - handler throwing an exception, save after 1" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      def atLeastOnceProjection(failWhenOffset: Long => Boolean = _ => false) =
        JdbcProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => new ConcatHandler(failWhenOffset))
          .withSaveOffset(1, Duration.Zero)

      offsetShouldBeEmpty()
      // run again up to safe point
      projectionTestKit.runWithTestSink(atLeastOnceProjection(_ == 4)) { sinkProbe =>
        sinkProbe.request(3)
        eventually {
          projectedValueShouldBe("e1|e2|e3")
          // we are saving after each envelope!
          offsetShouldBe(3)
        }
      }

      // run again up to failure point
      projectionTestKit.runWithTestSink(atLeastOnceProjection(_ == 4)) { sinkProbe =>
        sinkProbe.request(3)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        eventually {
          // we re-start from 3, so `e3` is not redelivered
          projectedValueShouldBe("e1|e2|e3")
          offsetShouldBe(3)
        }
      }
      // re-run projection without failing function
      projectionTestKit.run(atLeastOnceProjection()) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }

      offsetShouldBe(6L)
    }

    "restart from previous offset - handler throwing an exception, save after 2" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      offsetStore.readOffset[Long](projectionId).futureValue shouldBe empty

      def atLeastOnceProjection(failWhenOffset: Long => Boolean = _ => false) =
        JdbcProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => new ConcatHandler(failWhenOffset))
          .withSaveOffset(2, 1.minute)

      offsetShouldBeEmpty()
      // run again up to safe point
      projectionTestKit.runWithTestSink(atLeastOnceProjection(_ == 4)) { sinkProbe =>
        sinkProbe.request(3)
        eventually {
          projectedValueShouldBe("e1|e2|e3")
        }
      }

      // run again up to failure point
      projectionTestKit.runWithTestSink(atLeastOnceProjection(_ == 4)) { sinkProbe =>
        sinkProbe.request(4) // processes elem 3 again and fails when processing 4
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        eventually {
          // because failures, we may consume 'e1' and 'e2' more then once
          // we check that it at least starts with 'e1|e2|e3'
          projectedValueShould(_.startsWith("e1|e2|e3"))

          // note elem 3 is processed twice
          projectedValueShouldIncludeNTimes("e3", 2)
        }
      }

      // re-run projection without failing function
      projectionTestKit.run(atLeastOnceProjection()) {
        projectedValueShould(_.startsWith("e1|e2|e3"))
        projectedValueShould(_.endsWith("e3|e4|e5|e6"))

        // note elem 3 is should have been seen three times
        projectedValueShouldIncludeNTimes("e3", 3)

        offsetShouldBe(6L)
      }
    }

    "save offset after number of elements" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource[Envelope]()(system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val projection =
        JdbcProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = TestSourceProvider[Long, Envelope](source, _.offset),
            jdbcSessionFactory,
            handler = () => new ConcatHandler())
          .withSaveOffset(10, 1.minute)

      projectionTestKit.runWithTestSink(projection) { sinkProbe =>

        eventually {
          sourceProbe.get should not be null
        }
        sinkProbe.request(1000)

        (1 to 15).foreach { n =>
          sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
        }
        eventually {
          projectedValueShouldInclude("elem-15")
          offsetShouldBe(10L)
        }

        (16 to 22).foreach { n =>
          sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
        }
        eventually {
          projectedValueShouldInclude("elem-22")
          offsetShouldBe(20L)
        }
      }
    }

    "save offset after idle duration" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource[Envelope]()(system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val projection =
        JdbcProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = TestSourceProvider[Long, Envelope](source, _.offset),
            jdbcSessionFactory,
            handler = () => new ConcatHandler())
          .withSaveOffset(10, 2.seconds)

      projectionTestKit.runWithTestSink(projection) { sinkProbe =>

        eventually {
          sourceProbe.get should not be null
        }
        sinkProbe.request(1000)

        (1 to 15).foreach { n =>
          sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
        }
        eventually {
          projectedValueShouldInclude("elem-15")
          offsetShouldBe(10L)
        }

        (16 to 17).foreach { n =>
          sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
        }
        eventually {
          projectedValueShouldInclude("elem-17")
          offsetShouldBe(17L)
        }
      }

    }

    "verify offsets before processing an envelope" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()
      val verifiedProbe: TestProbe[Long] = createTestProbe[Long]()

      val testVerification = (offset: Long) => {
        verifiedProbe.ref ! offset
        VerificationSuccess
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        JdbcProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = testSourceProvider,
            jdbcSessionFactory,
            handler = () =>
              JdbcHandler[PureJdbcSession, Envelope] { (sess, envelope) =>
                verifiedProbe.expectMessage(envelope.offset)
                sess.withConnection { conn =>
                  TestRepository(conn).concatToText(envelope.id, envelope.message)
                }
              })

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
    }

    "skip record if offset verification fails before processing envelope" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val testVerification = (offset: Long) => {
        if (offset == 3L)
          VerificationFailure("test")
        else
          VerificationSuccess
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        JdbcProjection
          .atLeastOnce(
            projectionId,
            sourceProvider = testSourceProvider,
            jdbcSessionFactory,
            handler = () => new ConcatHandler())

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e4|e5|e6") // `e3` was skipped
      }
    }

    "handle async projection and store offset" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val result = new StringBuffer()

      def handler(): Handler[Envelope] = new Handler[Envelope] {
        override def process(envelope: Envelope): Future[Done] = {
          Future {
            result.append(envelope.message).append("|")
          }.map(_ => Done)
        }
      }

      val projection =
        JdbcProjection.atLeastOnceAsync(
          projectionId,
          sourceProvider = sourceProvider(entityId),
          jdbcSessionFactory,
          handler = () => handler())

      projectionTestKit.run(projection) {
        result.toString shouldBe "e1|e2|e3|e4|e5|e6|"
      }
      offsetShouldBe(6L)
    }
  }

  "A JDBC flow projection" must {

    "persist projection and offset" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      offsetShouldBeEmpty()

      val flowHandler =
        FlowWithContext[Envelope, ProjectionContext]
          .mapAsync(1) { env =>
            withRepo(_.concatToText(env.id, env.message))
          }

      val projection =
        JdbcProjection
          .atLeastOnceFlow(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = flowHandler)
          .withSaveOffset(1, 1.minute)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(6L)

    }
  }

  "JdbcProjection lifecycle" must {

    class LifecycleHandler(
        probe: ActorRef[String],
        failOnceOnOffset: AtomicInteger = new AtomicInteger(-1),
        alwaysFailOnOffset: Int = -1)
        extends JdbcHandler[Envelope, PureJdbcSession] {

      val createdMessage = "created"
      val startMessage = "start"
      val completedMessage = "completed"
      val failedMessage = "failed"

      // stop message can be 'completed' or 'failed'
      // that allows us to assert that the stopHandler is different execution paths were called in test
      private var stopMessage = completedMessage

      probe ! createdMessage

      override def start(): Unit = {
        // reset stop message to 'completed' on each new start
        stopMessage = completedMessage
        probe ! startMessage
      }

      override def stop(): Unit = {
        probe ! stopMessage
      }

      override def process(session: PureJdbcSession, envelope: Envelope): Unit = {
        if (envelope.offset == failOnceOnOffset.get()) {
          failOnceOnOffset.set(-1)
          stopMessage = failedMessage
          throw TestException(s"Fail $failOnceOnOffset")
        } else if (envelope.offset == alwaysFailOnOffset) {
          stopMessage = failedMessage
          throw TestException(s"Always Fail $alwaysFailOnOffset")
        } else {
          probe ! envelope.message
          ()
        }
      }
    }

    "call start and stop of the handler" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref)

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true)

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => handler)
          .withStatusObserver(statusObserver)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

      handlerProbe.expectMessage(handler.createdMessage)

      statusProbe.expectMessage(TestStatusObserver.Started)

      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("e1")
      handlerProbe.expectMessage("e2")
      handlerProbe.expectMessage("e3")
      handlerProbe.expectMessage("e4")
      handlerProbe.expectMessage("e5")
      handlerProbe.expectMessage("e6")
      // completed without failure
      handlerProbe.expectMessage(handler.completedMessage)
      handlerProbe.expectNoMessage() // no duplicate stop

      statusProbe.expectMessage(TestStatusObserver.Stopped)
      statusProbe.expectNoMessage()
    }

    "call start and stop of the handler when using TestKit.runWithTestSink" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref)

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => handler)

      // not using ProjectionTestKit because want to test restarts
      projectionTestKit.runWithTestSink(projection) { sinkProbe =>
        // request all 'strings' (e1 to e6)

        handlerProbe.expectMessage(handler.createdMessage)

        // the start happens inside runWithTestSink
        handlerProbe.expectMessage(handler.startMessage)

        // request the elements
        sinkProbe.request(6)
        handlerProbe.expectMessage("e1")
        handlerProbe.expectMessage("e2")
        handlerProbe.expectMessage("e3")
        handlerProbe.expectMessage("e4")
        handlerProbe.expectMessage("e5")
        handlerProbe.expectMessage("e6")

        // all elements should have reached the sink
        sinkProbe.expectNextN(6)
      }

      // completed without failure
      handlerProbe.expectMessage(handler.completedMessage)
      handlerProbe.expectNoMessage() // no duplicate stop
    }

    "call start and stop of handler when restarted" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

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
        JdbcProjection
          .exactlyOnce(projectionId, sourceProvider = sourceProvider(entityId), jdbcSessionFactory, handlerFactory)
          .withRestartBackoff(1.second, 2.seconds, 0.0)
          .withStatusObserver(statusObserver)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

      handlerProbe.expectMessage(handler.createdMessage)

      statusProbe.expectMessage(TestStatusObserver.Started)

      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("e1")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 1, "e1")))
      handlerProbe.expectMessage("e2")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 2, "e2")))
      handlerProbe.expectMessage("e3")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 3, "e3")))
      // fail 4
      handlerProbe.expectMessage(handler.failedMessage)
      val someTestException = TestException("err")
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "e4"), someTestException))

      // backoff will restart
      statusProbe.expectMessage(TestStatusObserver.Failed)
      statusProbe.expectMessage(TestStatusObserver.Stopped)
      handlerProbe.expectMessage(handler.createdMessage)
      handlerProbe.expectMessage(handler.startMessage)
      statusProbe.expectMessage(TestStatusObserver.Started)
      handlerProbe.expectMessage("e4")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 4, "e4")))
      handlerProbe.expectMessage("e5")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 5, "e5")))
      handlerProbe.expectMessage("e6")
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 6, "e6")))
      // now completed without failure
      handlerProbe.expectMessage(handler.completedMessage)
      handlerProbe.expectNoMessage() // no duplicate stop

      statusProbe.expectMessage(TestStatusObserver.Stopped)
      statusProbe.expectNoMessage()
    }

    "call start and stop of handler when failed but no restart" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val failOnceOnOffset = new AtomicInteger(4)
      val handler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset)

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => handler)
          .withRestartBackoff(1.second, 2.seconds, 0.0, maxRestarts = 0)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

      handlerProbe.expectMessage(handler.createdMessage)
      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("e1")
      handlerProbe.expectMessage("e2")
      handlerProbe.expectMessage("e3")
      // fail 4, not restarted
      // completed with failure
      handlerProbe.expectMessage(handler.failedMessage)
      handlerProbe.expectNoMessage() // no duplicate stop
    }

    "be able to stop when retrying" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref, alwaysFailOnOffset = 4)

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () => handler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(100, 100.millis))

      val ref = spawn(ProjectionBehavior(projection))

      handlerProbe.expectMessage(handler.createdMessage)
      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("e1")
      handlerProbe.expectMessage("e2")
      handlerProbe.expectMessage("e3")
      // fail 4

      // let it retry for a while
      Thread.sleep(300)

      ref ! ProjectionBehavior.Stop
      createTestProbe().expectTerminated(ref)
    }
  }

  "JdbcProjection management" must {

    "restart from beginning when offset is cleared" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () =>
              JdbcHandler[PureJdbcSession, Envelope] { (sess, envelope) =>
                sess.withConnection { conn =>
                  TestRepository(conn).concatToText(envelope.id, envelope.message)
                }
              })

      offsetShouldBeEmpty()

      // not using ProjectionTestKit because want to test ProjectionManagement
      spawn(ProjectionBehavior(projection))
      eventually {
        offsetShouldBe(6L)
      }
      ProjectionManagement(system).getOffset(projectionId).futureValue shouldBe Some(6L)
      projectedValueShouldBe("e1|e2|e3|e4|e5|e6")

      ProjectionManagement(system).clearOffset(projectionId).futureValue shouldBe Done
      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6|e1|e2|e3|e4|e5|e6")
      }
    }

    "restart from updated offset" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () =>
              JdbcHandler[PureJdbcSession, Envelope] { (sess, envelope) =>
                sess.withConnection { conn =>
                  TestRepository(conn).concatToText(envelope.id, envelope.message)
                }
              })

      offsetShouldBeEmpty()

      // not using ProjectionTestKit because want to test ProjectionManagement
      spawn(ProjectionBehavior(projection))

      eventually {
        offsetShouldBe(6L)
      }
      ProjectionManagement(system).getOffset(projectionId).futureValue shouldBe Some(6L)
      projectedValueShouldBe("e1|e2|e3|e4|e5|e6")

      ProjectionManagement(system).updateOffset(projectionId, 3L).futureValue shouldBe Done
      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6|e4|e5|e6")
      }
    }

    "pause projection" in {
      implicit val entityId = UUID.randomUUID().toString
      implicit val projectionId = genRandomProjectionId()

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(entityId),
            jdbcSessionFactory,
            handler = () =>
              JdbcHandler[PureJdbcSession, Envelope] { (sess, envelope) =>
                sess.withConnection { conn =>
                  TestRepository(conn).concatToText(envelope.id, envelope.message)
                }
              })

      offsetShouldBeEmpty()

      // not using ProjectionTestKit because want to test ProjectionManagement
      spawn(ProjectionBehavior(projection))

      val mgmt = ProjectionManagement(system)

      mgmt.isPaused(projectionId).futureValue shouldBe false

      eventually {
        offsetShouldBe(6L)
      }
      projectedValueShouldBe("e1|e2|e3|e4|e5|e6")

      mgmt.pause(projectionId).futureValue shouldBe Done
      mgmt.clearOffset(projectionId).futureValue shouldBe Done

      mgmt.isPaused(projectionId).futureValue shouldBe true

      Thread.sleep(500)
      // not updated because paused
      projectedValueShouldBe("e1|e2|e3|e4|e5|e6")

      mgmt.resume(projectionId)

      mgmt.isPaused(projectionId).futureValue shouldBe false

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6|e1|e2|e3|e4|e5|e6")
      }
    }
  }
}
