/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.collection.immutable
import scala.annotation.tailrec
import scala.compat.java8.FutureConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.cassandra.internal.CassandraOffsetStore
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSource
import org.scalatest.wordspec.AnyWordSpecLike

object CassandraProjectionSpec {
  case class Envelope(id: String, offset: Long, message: String)

  def offsetExtractor(env: Envelope): Long = env.offset

  def sourceProvider(system: ActorSystem[_], id: String): SourceProvider[Long, Envelope] = {

    val envelopes =
      List(
        Envelope(id, 1L, "abc"),
        Envelope(id, 2L, "def"),
        Envelope(id, 3L, "ghi"),
        Envelope(id, 4L, "jkl"),
        Envelope(id, 5L, "mno"),
        Envelope(id, 6L, "pqr"))

    TestSourceProvider(system, Source(envelopes))
  }

  case class TestSourceProvider(system: ActorSystem[_], src: Source[Envelope, _])
      extends SourceProvider[Long, Envelope] {
    implicit val executionContext: ExecutionContext = system.executionContext
    override def source(offset: () => Future[Option[Long]]): Future[Source[Envelope, _]] =
      offset().map {
        case Some(o) => src.dropWhile(_.offset <= o)
        case _       => src
      }

    override def extractOffset(env: Envelope): Long = env.offset
  }

  // test model is as simple as a text that gets other string concatenated to it
  case class ConcatStr(id: String, text: String) {
    def concat(newMsg: String): ConcatStr = copy(text = text + "|" + newMsg)
  }

  class TestRepository(session: CassandraSession)(implicit val ec: ExecutionContext) {
    val keyspace = "test"
    val table = "test_model"

    def concatToText(id: String, payload: String): Future[Done] = {
      for {
        concatStr <- findById(id).map {
          case Some(concatStr) => concatStr.concat(payload)
          case _               => ConcatStr(id, payload)
        }
        _ <- session.executeWrite(s"INSERT INTO $keyspace.$table (id, concatenated) VALUES (?, ?)", id, concatStr.text)
      } yield Done
    }

    def save(concatStr: ConcatStr): Future[Done] = {
      session.executeWrite(
        s"INSERT INTO $keyspace.$table (id, concatenated) VALUES (?, ?)",
        concatStr.id,
        concatStr.text)
    }

    def findById(id: String): Future[Option[ConcatStr]] = {
      session.selectOne(s"SELECT concatenated FROM $keyspace.$table WHERE id = ?", id).map {
        case Some(row) => Some(ConcatStr(id, row.getString("concatenated")))
        case None      => None
      }
    }

    def createKeyspaceAndTable(): Future[Done] = {
      session
        .executeDDL(
          s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }")
        .flatMap(_ => session.executeDDL(s"""
        |CREATE TABLE IF NOT EXISTS $keyspace.$table (
        |  id text,
        |  concatenated text,
        |  PRIMARY KEY (id))
        """.stripMargin.trim))
    }
  }

}

class CassandraProjectionSpec
    extends ScalaTestWithActorTestKit(ContainerSessionProvider.Config)
    with AnyWordSpecLike
    with LogCapturing {

  import CassandraProjectionSpec._

  private val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra")
  private implicit val ec: ExecutionContext = system.executionContext
  private val offsetStore = new CassandraOffsetStore(session)
  private val repository = new TestRepository(session)
  private val projectionTestKit = new ProjectionTestKit(testKit)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // don't use futureValue (patience) here because it can take a while to start the test container
    Await.result(ContainerSessionProvider.started, 30.seconds)

    Await.result(for {
      s <- session.underlying()
      // reason for setSchemaMetadataEnabled is that it speed up tests
      _ <- s.setSchemaMetadataEnabled(false).toScala
      _ <- offsetStore.createKeyspaceAndTable()
      _ <- repository.createKeyspaceAndTable()
      _ <- s.setSchemaMetadataEnabled(null).toScala
    } yield Done, 30.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.ready(for {
      s <- session.underlying()
      // reason for setSchemaMetadataEnabled is that it speed up tests
      _ <- s.setSchemaMetadataEnabled(false).toScala
      _ <- session.executeDDL(s"DROP keyspace ${offsetStore.keyspace}")
      _ <- session.executeDDL(s"DROP keyspace ${repository.keyspace}")
      _ <- s.setSchemaMetadataEnabled(null).toScala
    } yield Done, 30.seconds)
    super.afterAll()
  }

  private def genRandomProjectionId() =
    ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

  @tailrec private def eventuallyExpectError(sinkProbe: TestSubscriber.Probe[_]): Throwable = {
    sinkProbe.expectNextOrError() match {
      case Right(_)  => eventuallyExpectError(sinkProbe)
      case Left(exc) => exc
    }
  }

  private def concatHandler(): Handler[Envelope] = {
    Handler[Envelope](envelope => repository.concatToText(envelope.id, envelope.message))
  }

  private val concatHandlerFail4Msg = "fail on fourth envelope"

  class ConcatHandlerFail4 extends Handler[Envelope] {
    private val _attempts = new AtomicInteger()
    def attempts: Int = _attempts.get

    override def process(envelope: Envelope): Future[Done] = {
      if (envelope.offset == 4L) {
        _attempts.incrementAndGet()
        throw TestException(concatHandlerFail4Msg + s" after $attempts attempts")
      }
      repository.concatToText(envelope.id, envelope.message)
    }
  }

  private def concatHandlerFail4(): ConcatHandlerFail4 = new ConcatHandlerFail4

  "A Cassandra at-least-once projection" must {

    "persist projection and offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), concatHandler())
          .withSaveOffset(1, Duration.Zero)

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }

    "restart from previous offset - handler throwing an exception, save after 1" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val failingProjection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), concatHandlerFail4())
          .withSaveOffset(1, Duration.Zero)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(failingProjection) { sinkProbe =>
          sinkProbe.request(1000)
          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        }
      }
      withClue("check: projection is consumed up to third") {
        val concatStr = repository.findById(entityId).futureValue.get
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 3L
      }

      // re-run projection without failing function
      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), concatHandler())
          .withSaveOffset(1, Duration.Zero)

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }

    "restart from previous offset - handler throwing an exception, save after 2" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val failingProjection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), concatHandlerFail4())
          .withSaveOffset(2, 1.minute)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(failingProjection) { sinkProbe =>
          sinkProbe.request(1000)
          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        }
      }
      withClue("check: projection is consumed up to third") {
        val concatStr = repository.findById(entityId).futureValue.get
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 2L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 2L
      }

      // re-run projection without failing function
      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), concatHandler())
          .withSaveOffset(2, 1.minute)

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          // note that 3rd is duplicated
          concatStr.text shouldBe "abc|def|ghi|ghi|jkl|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }

    "save offset after number of envelopes" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource.probe[Envelope](system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, TestSourceProvider(system, source), concatHandler())
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
          repository.findById(entityId).futureValue.get.text should include("elem-15")
        }
        offsetStore.readOffset[Long](projectionId).futureValue.get shouldBe 10L

        (16 to 22).foreach { n =>
          sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
        }
        eventually {
          repository.findById(entityId).futureValue.get.text should include("elem-22")
        }
        offsetStore.readOffset[Long](projectionId).futureValue.get shouldBe 20L
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

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, TestSourceProvider(system, source), concatHandler())
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
          repository.findById(entityId).futureValue.get.text should include("elem-15")
        }
        offsetStore.readOffset[Long](projectionId).futureValue.get shouldBe 10L

        (16 to 17).foreach { n =>
          sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
        }
        eventually {
          offsetStore.readOffset[Long](projectionId).futureValue.get shouldBe 17L
        }
        repository.findById(entityId).futureValue.get.text should include("elem-17")

      }
    }

    "skip failing events when using RecoveryStrategy.skip" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), concatHandlerFail4())
          .withSaveOffset(2, 1.minute)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      projectionTestKit.run(projection) {
        withClue("checking: all expected values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          // note that 4th is skipped
          concatStr.text shouldBe "abc|def|ghi|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }

    "skip failing events after retrying when using RecoveryStrategy.retryAndSkip" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handler = concatHandlerFail4()

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), handler)
          .withSaveOffset(2, 1.minute)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndSkip(3, 10.millis))

      projectionTestKit.run(projection) {
        withClue("checking: all expected values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          // note that 4th is skipped
          concatStr.text shouldBe "abc|def|ghi|mno|pqr"
        }
      }

      withClue("check - event handler did failed 4 times") {
        // 1 + 3 => 1 original attempt and 3 retries
        handler.attempts shouldBe 1 + 3
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }

    "fail after retrying when using RecoveryStrategy.retryAndFail" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handler = concatHandlerFail4()

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), handler)
          .withSaveOffset(2, 1.minute)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(3, 10.millis))

      intercept[TestException] {
        projectionTestKit.run(projection) {
          withClue("checking: all expected values were concatenated") {
            val concatStr = repository.findById(entityId).futureValue.get
            concatStr.text shouldBe "abc|def|ghi"
          }
        }
      }

      withClue("check - event handler did failed 4 times") {
        // 1 + 3 => 1 original attempt and 3 retries
        handler.attempts shouldBe 1 + 3
      }
    }
  }

  "A Cassandra grouped projection" must {

    "persist projection and offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val groupedHandler: Handler[immutable.Seq[Envelope]] = new Handler[immutable.Seq[Envelope]] {
        private var state: Future[Option[ConcatStr]] = repository.findById(entityId)

        override def process(group: immutable.Seq[Envelope]): Future[Done] = {
          val newState = state.flatMap { s =>
            val concatStr = group.foldLeft(s) {
              case (None, env)      => Some(ConcatStr(env.id, env.message))
              case (Some(acc), env) => Some(acc.concat(env.message))
            }
            val ok = concatStr match {

              case Some(c) => repository.save(c)
              case None    => Future.successful(Done)
            }
            ok.map(_ => concatStr)
          }
          state = newState
          newState.map(_ => Done)
        }
      }

      val projection =
        CassandraProjection
          .groupedWithin[Long, Envelope](projectionId, sourceProvider(system, entityId), groupedHandler)
          .withGroup(groupAfterEnvelopes = 3, groupAfterDuration = 1.minute)

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }
  }

  "A Cassandra at-most-once projection" must {

    "persist projection and offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val projection =
        CassandraProjection
          .atMostOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), concatHandler())

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }

    "restart from next offset - handler throwing an exception" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val failingProjection =
        CassandraProjection
          .atMostOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), concatHandlerFail4())

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(failingProjection) { sinkProbe =>
          sinkProbe.request(1000)
          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        }
      }
      withClue("check: projection is consumed up to third") {
        val concatStr = repository.findById(entityId).futureValue.get
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 4L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 4L // offset saved before handler for at-most-once
      }

      // re-run projection without failing function
      val projection =
        CassandraProjection.atMostOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), concatHandler())

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          // failed: jkl not included
          concatStr.text shouldBe "abc|def|ghi|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }
  }

  "CassandraProjection lifecycle" must {

    class LifecycleHandler(probe: ActorRef[String], failOnceOnOffset: Int) extends Handler[Envelope] {

      private var failedOnce = false
      val startMessage = "start"
      val completedMessage = "completed"
      val failedMessage = "failed"

      // stop message can be 'completed' or 'failed'
      // that allows us to assert that the stopHandler is different execution paths were called in test
      private var stopMessage = completedMessage

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

      override def process(envelope: Envelope): Future[Done] = {
        if (envelope.offset == failOnceOnOffset && !failedOnce) {
          failedOnce = true
          stopMessage = failedMessage
          throw TestException(s"Fail $failOnceOnOffset")
        } else {
          probe ! envelope.message
          Future.successful(Done)
        }
      }
    }

    "call start and stop of the handler" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset = -1)

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), handler)
          .withSaveOffset(1, Duration.Zero)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

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
    }

    "call start and stop of the handler when using TestKit.runWithTestSink" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset = -1)

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), handler)
          .withSaveOffset(1, Duration.Zero)

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

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), handler)
          .withSettings(ProjectionSettings(system).withBackoff(1.second, 2.seconds, 0.0))
          .withSaveOffset(1, Duration.Zero)

      // not using ProjectionTestKit because want to test restarts
      spawn(ProjectionBehavior(projection))

      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("abc")
      handlerProbe.expectMessage("def")
      handlerProbe.expectMessage("ghi")
      // fail 4
      handlerProbe.expectMessage(handler.failedMessage)

      // backoff will restart
      handlerProbe.expectMessage(handler.startMessage)
      handlerProbe.expectMessage("jkl")
      handlerProbe.expectMessage("mno")
      handlerProbe.expectMessage("pqr")
      // now completed without failure
      handlerProbe.expectMessage(handler.completedMessage)
      handlerProbe.expectNoMessage() // no duplicate stop
    }

    "call start and stop of handler when failed but no restart" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref, failOnceOnOffset = 4)

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(system, entityId), handler)
          .withSettings(
            ProjectionSettings(system).withBackoff(1.second, 2.seconds, 0.0, maxRestarts = 0)
          ) // no restarts
          .withSaveOffset(1, Duration.Zero)

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
  }
}
