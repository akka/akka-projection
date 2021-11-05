/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

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
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.projection.HandlerRecoveryStrategy
import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.TestStatusObserver
import akka.projection.r2dbc.internal.R2dbcOffsetStore
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.r2dbc.scaladsl.R2dbcSession
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
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object R2dbcProjectionSpec {
  final case class Envelope(id: String, offset: Long, message: String)

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
    def concat(newMsg: String): ConcatStr = {
      if (text == "")
        copy(id, newMsg)
      else
        copy(text = text + "|" + newMsg)
    }
  }

  object TestRepository {
    val table = "projection_spec_model"

    val createTableSql: String =
      s"""|CREATE table IF NOT EXISTS "$table" (
          |  id VARCHAR(255) NOT NULL,
          |  concatenated VARCHAR(255) NOT NULL,
          |  PRIMARY KEY(id)
          |);""".stripMargin
  }

  final case class TestRepository(session: R2dbcSession)(implicit ec: ExecutionContext, system: ActorSystem[_]) {
    import TestRepository.table

    private val logger = LoggerFactory.getLogger(this.getClass)

    def concatToText(id: String, payload: String): Future[Done] = {
      val savedStrOpt = findById(id)

      savedStrOpt.flatMap { strOpt =>
        val newConcatStr = strOpt
          .map {
            _.concat(payload)
          }
          .getOrElse(ConcatStr(id, payload))

        upsert(newConcatStr)
      }
    }

    def update(id: String, payload: String): Future[Done] = {
      upsert(ConcatStr(id, payload))
    }

    def updateWithNullValue(id: String): Future[Done] = {
      upsert(ConcatStr(id, null))
    }

    private def upsert(concatStr: ConcatStr): Future[Done] = {
      logger.debug("TestRepository.upsert: [{}]", concatStr)

      val stmtSql =
        s"""|
         |INSERT INTO "$table" (id, concatenated)  VALUES ($$1, $$2)
         |ON CONFLICT (id)
         |DO UPDATE SET
         |  id = excluded.id,
         |  concatenated = excluded.concatenated
         |""".stripMargin
      val stmt = session
        .createStatement(stmtSql)
        .bind(0, concatStr.id)
        .bind(1, concatStr.text)

      R2dbcExecutor
        .updateOneInTx(stmt)
        .map(_ => Done)
    }

    def findById(id: String): Future[Option[ConcatStr]] = {
      logger.debug("TestRepository.findById: [{}]", id)

      val stmtSql = s"SELECT * FROM $table WHERE id = $$1"
      val stmt = session
        .createStatement(stmtSql)
        .bind(0, id)

      R2dbcExecutor.selectOneInTx(
        stmt,
        row => ConcatStr(row.get("id", classOf[String]), row.get("concatenated", classOf[String])))
    }

  }
}

class R2dbcProjectionSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import R2dbcProjectionSpec._

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext

  private val logger = LoggerFactory.getLogger(getClass)
  private val settings = R2dbcProjectionSettings(testKit.system)
  private def createOffsetStore(projectionId: ProjectionId): R2dbcOffsetStore =
    new R2dbcOffsetStore(projectionId, None, system, settings, r2dbcExecutor)
  private val projectionTestKit = ProjectionTestKit(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    Await.result(
      r2dbcExecutor.executeDdl("beforeAll createTable") { conn =>
        conn.createStatement(TestRepository.createTableSql)
      },
      10.seconds)
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(_.createStatement(s"delete from ${TestRepository.table}")),
      10.seconds)
  }

  private def offsetShouldBe(expected: Long)(implicit offsetStore: R2dbcOffsetStore) = {
    offsetStore.readOffset[Long]().futureValue shouldBe Some(expected)
  }

  private def offsetShouldBeEmpty()(implicit offsetStore: R2dbcOffsetStore) = {
    offsetStore.readOffset[Long]().futureValue shouldBe empty
  }

  private def projectedValueShouldBe(expected: String)(implicit entityId: String) = {
    val opt = withRepo(_.findById(entityId)).futureValue.map(_.text)
    opt shouldBe Some(expected)
  }

  private def projectedValueShouldInclude(expected: String)(implicit entityId: String) = {
    withClue(s"checking projected value contains [$expected]: ") {
      val text = withRepo(_.findById(entityId)).futureValue.get.text
      text should include(expected)
    }
  }

  private def projectedValueShould(actual: String => Boolean)(implicit entityId: String) = {
    withClue(s"checking projected value fulfil predicate: ") {
      val text = withRepo(_.findById(entityId)).futureValue.get.text
      actual(text)
    }
  }

  private def projectedValueShouldIncludeNTimes(expected: String, nTimes: Int)(implicit entityId: String) = {
    withClue(s"checking projected value contains [$expected] $nTimes times: ") {
      val text = withRepo(_.findById(entityId)).futureValue.get.text
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

  private def withRepo[R](fun: TestRepository => Future[R]): Future[R] = {
    r2dbcExecutor.withConnection("test") { conn =>
      val session = new R2dbcSession(conn)
      fun(TestRepository(session))
    }
  }

  class ConcatHandler(failPredicate: Long => Boolean = _ => false) extends R2dbcHandler[Envelope] {

    private val _attempts = new AtomicInteger()
    def attempts: Int = _attempts.get

    override def process(session: R2dbcSession, envelope: Envelope): Future[Done] = {
      if (failPredicate(envelope.offset)) {
        _attempts.incrementAndGet()
        throw TestException(concatHandlerFail4Msg + s" after $attempts attempts")
      } else {
        logger.debug("handling {}", envelope)
        TestRepository(session).concatToText(envelope.id, envelope.message)
      }
    }
  }

  "A R2DBC exactly-once projection" must {

    "persist projection and offset in the same write operation (transactional)" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val projection =
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(entityId),
          handler = () => new ConcatHandler)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(6L)
    }

    "skip failing events when using RecoveryStrategy.skip" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val bogusEventHandler = new ConcatHandler(_ == 4)

      val projectionFailing =
        R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
            handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        projectedValueShouldBe("e1|e2|e3|e5|e6")
      }
      offsetShouldBe(6L)
    }

    "skip failing events after retrying when using RecoveryStrategy.retryAndSkip" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val bogusEventHandler = new ConcatHandler(_ == 4)

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val progressProbe = createTestProbe[TestStatusObserver.OffsetProgress[Envelope]]()
      val statusObserver =
        new TestStatusObserver[Envelope](statusProbe.ref, offsetProgressProbe = Some(progressProbe.ref))

      val projectionFailing =
        R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
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
      // Offset 4 is not stored so it is not reported.
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(entityId, 5, "e5")))

      offsetShouldBe(6L)
    }

    "fail after retrying when using RecoveryStrategy.retryAndFail" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val bogusEventHandler = new ConcatHandler(_ == 4)

      val projectionFailing =
        R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
            handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(3, 10.millis))

      offsetShouldBeEmpty()
      projectionTestKit.runWithTestSink(projectionFailing) { sinkProbe =>
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
      }
      projectedValueShouldBe("e1|e2|e3")
      // 1 + 3 => 1 original attempt and 3 retries
      bogusEventHandler.attempts shouldBe 1 + 3
      offsetShouldBe(3L)
    }

    "restart from previous offset - fail with throwing an exception" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      def exactlyOnceProjection(failWhenOffset: Long => Boolean = _ => false) = {
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(entityId),
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val bogusEventHandler = new R2dbcHandler[Envelope] {
        override def process(session: R2dbcSession, envelope: Envelope): Future[Done] = {
          val repo = TestRepository(session)
          if (envelope.offset == 4L) repo.updateWithNullValue(envelope.id)
          else repo.concatToText(envelope.id, envelope.message)
        }
      }

      def exactlyOnceProjection(handler: () => R2dbcHandler[Envelope]) =
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(entityId),
          handler = handler)

      offsetShouldBeEmpty()
      projectionTestKit.runWithTestSink(exactlyOnceProjection(() => bogusEventHandler)) { sinkProbe =>
        sinkProbe.request(4)
        val exc = eventuallyExpectError(sinkProbe)
        exc.getMessage shouldBe "value must not be null"
        exc.getClass shouldBe classOf[IllegalArgumentException]
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      case class ProbeMessage(str: String, offset: Long)
      val verificationProbe = createTestProbe[ProbeMessage]("verification")
      val processProbe = createTestProbe[ProbeMessage]("processing")

      val testVerification = (offset: Long) => {
        verificationProbe.ref ! ProbeMessage("verification", offset)
        VerificationSuccess
      }

      val handler = new R2dbcHandler[Envelope] {
        override def process(session: R2dbcSession, envelope: Envelope): Future[Done] = {

          verificationProbe.receiveMessage().offset shouldEqual envelope.offset
          processProbe.ref ! ProbeMessage("process", envelope.offset)

          TestRepository(session).concatToText(envelope.id, envelope.message)
        }
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = testSourceProvider,
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val testVerification = (offset: Long) => {
        if (offset == 3L)
          VerificationFailure("test")
        else
          VerificationSuccess
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = testSourceProvider,
          handler = () => new ConcatHandler())

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e4|e5|e6") // `e3` was skipped
      }
    }

    "skip record if offset verification fails after processing envelope" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val testVerification = (offset: Long) => {
        if (offset == 3L)
          VerificationFailure("test")
        else
          VerificationSuccess
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = testSourceProvider,
          handler = () => new ConcatHandler())

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e4|e5|e6") // `e3` was skipped
      }
    }
  }

  "A R2DBC grouped projection" must {
    "persist projection and offset in the same write operation (transactional)" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val handlerCalled = "called"
      val handlerProbe = createTestProbe[String]("calls-to-handler")

      val projection =
        R2dbcProjection
          .groupedWithin(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
            handler = () =>
              R2dbcHandler[immutable.Seq[Envelope]] { (session, envelopes) =>
                handlerProbe.ref ! handlerCalled
                if (envelopes.isEmpty)
                  Future.successful(Done)
                else {
                  val repo = TestRepository(session)
                  val id = envelopes.head.id
                  repo.findById(id).flatMap { existing =>
                    val newConcatStr = envelopes.foldLeft(existing.getOrElse(ConcatStr(id, ""))) { (acc, env) =>
                      acc.concat(env.message)
                    }
                    repo.update(id, newConcatStr.text)
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val result = new StringBuffer()

      def handler(): Handler[immutable.Seq[Envelope]] = new Handler[immutable.Seq[Envelope]] {
        override def process(envelopes: immutable.Seq[Envelope]): Future[Done] = {
          Future {
            envelopes.foreach(env => result.append(env.message).append("|"))
          }.map(_ => Done)
        }
      }

      val projection =
        R2dbcProjection
          .groupedWithinAsync(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
            handler = () => handler())
          .withGroup(2, 3.seconds)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        result.toString shouldBe "e1|e2|e3|e4|e5|e6|"
      }
      offsetShouldBe(6L)
    }
  }

  "A R2DBC at-least-once projection" must {

    "persist projection and offset" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val projection =
        R2dbcProjection.atLeastOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(entityId),
          handler = () => new ConcatHandler)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(6L)
    }

    "skip failing events when using RecoveryStrategy.skip, save after 1" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val projection =
        R2dbcProjection
          .atLeastOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val projection =
        R2dbcProjection
          .atLeastOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      def atLeastOnceProjection(failWhenOffset: Long => Boolean = _ => false) =
        R2dbcProjection
          .atLeastOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      offsetStore.readOffset[Long]().futureValue shouldBe empty

      def atLeastOnceProjection(failWhenOffset: Long => Boolean = _ => false) =
        R2dbcProjection
          .atLeastOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource.probe[Envelope](system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val projection =
        R2dbcProjection
          .atLeastOnce(
            projectionId,
            Some(settings),
            sourceProvider = TestSourceProvider[Long, Envelope](source, _.offset),
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource.probe[Envelope](system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val projection =
        R2dbcProjection
          .atLeastOnce(
            projectionId,
            Some(settings),
            sourceProvider = TestSourceProvider[Long, Envelope](source, _.offset),
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)
      val verifiedProbe = createTestProbe[Long]()

      val testVerification = (offset: Long) => {
        verifiedProbe.ref ! offset
        VerificationSuccess
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        R2dbcProjection
          .atLeastOnce(
            projectionId,
            Some(settings),
            sourceProvider = testSourceProvider,
            handler = () =>
              R2dbcHandler[Envelope] { (session, envelope) =>
                verifiedProbe.expectMessage(envelope.offset)
                TestRepository(session).concatToText(envelope.id, envelope.message)
              })

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
    }

    "skip record if offset verification fails before processing envelope" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val testVerification = (offset: Long) => {
        if (offset == 3L)
          VerificationFailure("test")
        else
          VerificationSuccess
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        R2dbcProjection
          .atLeastOnce(
            projectionId,
            Some(settings),
            sourceProvider = testSourceProvider,
            handler = () => new ConcatHandler())

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e4|e5|e6") // `e3` was skipped
      }
    }

    "handle async projection and store offset" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val result = new StringBuffer()

      def handler(): Handler[Envelope] = new Handler[Envelope] {
        override def process(envelope: Envelope): Future[Done] = {
          Future {
            result.append(envelope.message).append("|")
          }.map(_ => Done)
        }
      }

      val projection =
        R2dbcProjection.atLeastOnceAsync(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(entityId),
          handler = () => handler())

      projectionTestKit.run(projection) {
        result.toString shouldBe "e1|e2|e3|e4|e5|e6|"
      }
      offsetShouldBe(6L)
    }
  }

  "A R2DBC flow projection" must {

    "persist projection and offset" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      offsetShouldBeEmpty()

      val flowHandler =
        FlowWithContext[Envelope, ProjectionContext]
          .mapAsync(1) { env =>
            withRepo(_.concatToText(env.id, env.message))
          }

      val projection =
        R2dbcProjection
          .atLeastOnceFlow(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
            handler = flowHandler)
          .withSaveOffset(1, 1.minute)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(6L)

    }
  }

  "R2dbcProjection lifecycle" must {

    class LifecycleHandler(
        probe: ActorRef[String],
        failOnceOnOffset: AtomicInteger = new AtomicInteger(-1),
        alwaysFailOnOffset: Int = -1)
        extends R2dbcHandler[Envelope] {

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

      override def process(session: R2dbcSession, envelope: Envelope): Future[Done] = {
        if (envelope.offset == failOnceOnOffset.get()) {
          failOnceOnOffset.set(-1)
          stopMessage = failedMessage
          throw TestException(s"Fail $failOnceOnOffset")
        } else if (envelope.offset == alwaysFailOnOffset) {
          stopMessage = failedMessage
          throw TestException(s"Always Fail $alwaysFailOnOffset")
        } else {
          probe ! envelope.message
          Future.successful(Done)
        }
      }
    }

    "call start and stop of the handler" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref)

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true)

      val projection =
        R2dbcProjection
          .exactlyOnce(projectionId, Some(settings), sourceProvider = sourceProvider(entityId), handler = () => handler)
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref)

      val projection =
        R2dbcProjection
          .exactlyOnce(projectionId, Some(settings), sourceProvider = sourceProvider(entityId), handler = () => handler)

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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

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
        R2dbcProjection
          .exactlyOnce(projectionId, Some(settings), sourceProvider = sourceProvider(entityId), handlerFactory)
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val handlerProbe = createTestProbe[String]()
      val failOnceOnOffset = new AtomicInteger(4)
      val handler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset)

      val projection =
        R2dbcProjection
          .exactlyOnce(projectionId, Some(settings), sourceProvider = sourceProvider(entityId), handler = () => handler)
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref, alwaysFailOnOffset = 4)

      val projection =
        R2dbcProjection
          .exactlyOnce(projectionId, Some(settings), sourceProvider = sourceProvider(entityId), handler = () => handler)
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

  "R2dbcProjection management" must {

    "restart from beginning when offset is cleared" in {
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val projection =
        R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
            handler = () =>
              R2dbcHandler[Envelope] { (session, envelope) =>
                TestRepository(session).concatToText(envelope.id, envelope.message)
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
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val projection =
        R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
            handler = () =>
              R2dbcHandler[Envelope] { (session, envelope) =>
                TestRepository(session).concatToText(envelope.id, envelope.message)
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
      pending // FIXME not implemented yet
      implicit val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val projection =
        R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(entityId),
            handler = () =>
              R2dbcHandler[Envelope] { (session, envelope) =>
                TestRepository(session).concatToText(envelope.id, envelope.message)
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
