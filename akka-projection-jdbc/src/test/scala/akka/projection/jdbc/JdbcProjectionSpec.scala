/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.util.Optional
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

import scala.annotation.tailrec
import scala.compat.java8.FutureConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.japi.function
import akka.japi.function.Creator
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.TestStatusObserver
import akka.projection.javadsl.SourceProvider
import akka.projection.jdbc.JdbcProjectionSpec.Envelope
import akka.projection.jdbc.JdbcProjectionSpec.PureJdbcSession
import akka.projection.jdbc.JdbcProjectionSpec.TestRepository
import akka.projection.jdbc.JdbcProjectionSpec.jdbcSessionCreator
import akka.projection.jdbc.JdbcProjectionSpec.jdbcSessionFactory
import akka.projection.jdbc.JdbcProjectionSpec.sourceProvider
import akka.projection.jdbc.internal.JdbcOffsetStore
import akka.projection.jdbc.internal.JdbcSettings
import akka.projection.jdbc.javadsl.JdbcHandler
import akka.projection.jdbc.javadsl.JdbcProjection
import akka.projection.jdbc.javadsl.JdbcSession
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.stream.javadsl.Source
import akka.stream.scaladsl.{ Source => ScalaSource }
import akka.stream.testkit.TestSubscriber
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException
import org.scalatest.OptionValues
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object JdbcProjectionSpec {
  val config: Config = ConfigFactory.parseString("""
    akka.projection.jdbc = {
      dialect = "h2-dialect"
      offset-store {
        schema = ""
        table = "AKKA_PROJECTION_OFFSET_STORE"
      }
      
      # TODO: configure a connection pool for the tests
      blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 5
    }
    """)

  class PureJdbcSession extends JdbcSession {

    lazy val conn = {
      Class.forName("org.h2.Driver")
      val c = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
      c.setAutoCommit(false)
      c
    }

    override def withConnection[Result](func: function.Function[Connection, Result]): Result =
      func(conn)

    override def commit(): Unit = conn.commit()

    override def rollback(): Unit = conn.rollback()

    override def close(): Unit = conn.close()
  }

  // Java API requires a Creator
  val jdbcSessionCreator: Creator[PureJdbcSession] = () => new PureJdbcSession
  // internal API needs Creator adapted to Scala function
  val jdbcSessionFactory: () => PureJdbcSession = jdbcSessionCreator.create _

  case class Envelope(id: String, offset: Long, message: String)

  def sourceProvider(system: ActorSystem[_], id: String, complete: Boolean = true): SourceProvider[Long, Envelope] = {

    val envelopes =
      List(
        Envelope(id, 1L, "abc"),
        Envelope(id, 2L, "def"),
        Envelope(id, 3L, "ghi"),
        Envelope(id, 4L, "jkl"),
        Envelope(id, 5L, "mno"),
        Envelope(id, 6L, "pqr"))

    val src = if (complete) ScalaSource(envelopes) else ScalaSource(envelopes).concat(ScalaSource.maybe)
    TestSourceProvider(system, src.asJava)
  }

  case class TestSourceProvider(system: ActorSystem[_], src: Source[Envelope, _])
      extends SourceProvider[Long, Envelope] {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def source(offset: Supplier[CompletionStage[Optional[Long]]]): CompletionStage[Source[Envelope, _]] = {
      offset
        .get()
        .toScala
        .map { offsetOpt =>
          if (offsetOpt.isPresent) src.dropWhile(_.offset <= offsetOpt.get())
          else src
        }
        .toJava
    }

    override def extractOffset(env: Envelope): Long = env.offset
  }
  // test model is as simple as a text that gets other string concatenated to it
  case class ConcatStr(id: String, text: String) {
    def concat(newMsg: String) = copy(text = text + "|" + newMsg)
  }

  case class TestRepository(conn: Connection) {

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
      val stmtStr = s"""MERGE INTO "$table" ("ID","CONCATENATED")  VALUES (?,?)"""
      JdbcSession.tryWithResource(conn.prepareStatement(stmtStr)) { stmt =>
        stmt.setString(1, concatStr.id)
        stmt.setString(2, concatStr.text)
        stmt.executeUpdate()
        Done
      }
    }

    def findById(id: String): Option[ConcatStr] = {

      val stmtStr = s"SELECT * FROM $table WHERE ID = ?"

      JdbcSession.tryWithResource(conn.prepareStatement(stmtStr)) { stmt =>
        stmt.setString(1, id)
        val resultSet = stmt.executeQuery()
        if (resultSet.first()) {
          Some(ConcatStr(resultSet.getString("ID"), resultSet.getString("CONCATENATED")))
        } else None
      }
    }

    def readValue(id: String): String = ???

    def createTable(): Done = {

      val createTableStatement = s"""
       create table if not exists "$table" (
        "ID" CHAR(255) NOT NULL,
        "CONCATENATED" CHAR(255) NOT NULL
       );
       """

      val alterTableStatement =
        s"""alter table "$table" add constraint "PK_ID" primary key("ID");"""

      JdbcSession.tryWithResource(conn.createStatement()) { stmt =>
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

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val actorSystem: ActorSystem[Nothing] = testKit.system
  implicit val executionContext: ExecutionContext = testKit.system.executionContext

  val jdbcSettings = JdbcSettings(testKit.system)
  val offsetStore = new JdbcOffsetStore(jdbcSettings, jdbcSessionFactory)

  val projectionTestKit = new ProjectionTestKit(testKit)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // create offset table
    val creationFut = offsetStore
      .createIfNotExists()
      .flatMap(_ =>
        JdbcSession.withConnection(jdbcSessionFactory) { conn =>
          TestRepository(conn).createTable()
        })
    Await.result(creationFut, 3.seconds)
  }

  private def genRandomProjectionId() =
    ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

  // TODO: extract this to some utility
  @tailrec private def eventuallyExpectError(sinkProbe: TestSubscriber.Probe[_]): Throwable = {
    sinkProbe.expectNextOrError() match {
      case Right(_)  => eventuallyExpectError(sinkProbe)
      case Left(exc) => exc
    }
  }

  private val concatHandlerFail4Msg = "fail on fourth envelope"

  private def withRepo[R](block: TestRepository => R): Future[R] = {
    JdbcSession.withConnection(jdbcSessionFactory) { conn =>
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
          TestRepository(conn).concatToText(envelope.id, envelope.message)
        }
      }
    }
  }

  "A JDBC exactly-once projection" must {

    "persist projection and offset in the same write operation (transactional)" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val projection =
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(system, entityId),
          jdbcSessionCreator,
          handler = new ConcatHandler)

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = withRepo(_.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }

      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "skip failing events when using RecoveryStrategy.skip" in {
      pending
    }

    "skip failing events after retrying when using RecoveryStrategy.retryAndSkip" in {
      pending
    }

    "fail after retrying when using RecoveryStrategy.retryAndFail" in {
      pending
    }

    "restart from previous offset - fail with throwing an exception" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val bogusEventHandler = new ConcatHandler(_ == 4)

      val projectionFailing =
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(system, entityId),
          jdbcSessionCreator,
          handler = bogusEventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(projectionFailing) { sinkProbe =>
          sinkProbe.request(1000)
          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        }
      }
      withClue("check: projection is consumed up to third") {
        val concatStr = withRepo(_.findById(entityId)).futureValue.value
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 3L
      }

      // re-run projection without failing function
      val projection =
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(system, entityId),
          jdbcSessionCreator,
          handler = new ConcatHandler())

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = withRepo(_.findById(entityId)).futureValue.value
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

      val bogusEventHandler = new JdbcHandler[Envelope, PureJdbcSession] {
        override def process(session: PureJdbcSession, envelope: Envelope): Unit = {
          session.withConnection { conn =>
            val repo = TestRepository(conn)
            if (envelope.offset == 4L) repo.updateWithNullValue(envelope.id)
            else repo.concatToText(envelope.id, envelope.message)
          }
        }
      }

      val projectionFailing =
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(system, entityId),
          jdbcSessionCreator,
          handler = bogusEventHandler)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      projectionTestKit.runWithTestSink(projectionFailing) { sinkProbe =>
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getClass shouldBe classOf[JdbcSQLIntegrityConstraintViolationException]
      }

      withClue("check: projection is consumed up to third") {
        val concatStr = withRepo(_.findById(entityId)).futureValue.value
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 3L
      }

      // re-run projection without failing function
      val projection =
        JdbcProjection.exactlyOnce(
          projectionId,
          sourceProvider = sourceProvider(system, entityId),
          jdbcSessionCreator,
          handler = new ConcatHandler())

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = withRepo(_.findById(entityId)).futureValue.value
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "verify offsets before and after processing an envelope" in {
      pending
    }

    "skip record if offset verification fails before processing envelope" in {
      pending
    }

    "skip record if offset verification fails after processing envelope" in {
      pending
    }
  }

  "A JDBC grouped projection" must {
    "persist projection and offset in the same write operation (transactional)" in {
      pending
    }
  }

  "A JDBC at-least-once projection" must {

    "persist projection and offset" in {
      pending
    }

    "skip failing events when using RecoveryStrategy.skip, save after 1" in {
      pending
    }

    "skip failing events when using RecoveryStrategy.skip, save after 2" in {
      pending
    }

    "restart from previous offset - handler throwing an exception, save after 1" in {
      pending
    }

    "restart from previous offset - handler throwing an exception, save after 2" in {
      pending
    }

    "save offset after number of elements" in {
      pending
    }

    "save offset after idle duration" in {
      pending
    }

    "verify offsets before processing an envelope" in {
      pending
    }

    "skip record if offset verification fails before processing envelope" in {
      pending
    }
  }

  "A JDBC flow projection" must {

    "persist projection and offset" in {
      pending
    }
  }

  "JdbcProjection lifecycle" must {

    class LifecycleHandler(probe: ActorRef[String], failOnceOnOffset: Int = -1, alwaysFailOnOffset: Int = -1)
        extends JdbcHandler[Envelope, PureJdbcSession] {

      private var failedOnce = false
      val startMessage = "start"
      val completedMessage = "completed"
      val failedMessage = "failed"

      // stop message can be 'completed' or 'failed'
      // that allows us to assert that the stopHandler is different execution paths were called in test
      private var stopMessage = completedMessage

      override def start(): CompletionStage[Done] = {
        // reset stop message to 'completed' on each new start
        stopMessage = completedMessage
        probe ! startMessage
        CompletableFuture.completedFuture(Done)
      }

      override def stop(): CompletionStage[Done] = {
        probe ! stopMessage
        CompletableFuture.completedFuture(Done)
      }

      override def process(session: PureJdbcSession, envelope: Envelope): Unit = {
        if (envelope.offset == failOnceOnOffset && !failedOnce) {
          failedOnce = true
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
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset = -1)

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true)

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(system, entityId),
            jdbcSessionCreator,
            handler = handler)
          .withStatusObserver(statusObserver)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

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
      val handler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset = -1)

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(system, entityId),
            jdbcSessionCreator,
            handler = handler)

      // not using ProjectionTestKit because want to test restarts
      projectionTestKit.runWithTestSink(projection) { sinkProbe =>
        // request all 'strings' (abc to pqr)

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
      val handler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset = 4)

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val progressProbe = createTestProbe[TestStatusObserver.Progress[Envelope]]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true, Some(progressProbe.ref))

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(system, entityId),
            jdbcSessionCreator,
            handler = handler)
          .withRestartBackoff(1.second, 2.seconds, 0.0)
          .withStatusObserver(statusObserver)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

      statusProbe.expectMessage(TestStatusObserver.Started)

      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("abc")
      progressProbe.expectMessage(TestStatusObserver.Progress(Envelope(entityId, 1, "abc")))
      handlerProbe.expectMessage("def")
      progressProbe.expectMessage(TestStatusObserver.Progress(Envelope(entityId, 2, "def")))
      handlerProbe.expectMessage("ghi")
      progressProbe.expectMessage(TestStatusObserver.Progress(Envelope(entityId, 3, "ghi")))
      // fail 4
      handlerProbe.expectMessage(handler.failedMessage)
      val someTestException = TestException("err")
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))

      // backoff will restart
      statusProbe.expectMessage(TestStatusObserver.Stopped)
      statusProbe.expectMessage(TestStatusObserver.Failed)
      statusProbe.expectMessage(TestStatusObserver.Started)
      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("jkl")
      progressProbe.expectMessage(TestStatusObserver.Progress(Envelope(entityId, 4, "jkl")))
      handlerProbe.expectMessage("mno")
      progressProbe.expectMessage(TestStatusObserver.Progress(Envelope(entityId, 5, "mno")))
      handlerProbe.expectMessage("pqr")
      progressProbe.expectMessage(TestStatusObserver.Progress(Envelope(entityId, 6, "pqr")))
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
      val handler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset = 4)

      val projection =
        JdbcProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = sourceProvider(system, entityId),
            jdbcSessionCreator,
            handler = handler)
          .withRestartBackoff(1.second, 2.seconds, 0.0, maxRestarts = 0)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

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
      pending // needs support for retrying strategy
    }
  }

  "JdbcProjection management" must {

    "restart from beginning when offset is cleared" in {
      pending
    }

    "restart from updated offset" in {
      pending
    }
  }
}
