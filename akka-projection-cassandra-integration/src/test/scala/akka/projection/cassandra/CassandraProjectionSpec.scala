/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.jdk.FutureConverters._

import akka.Done
import akka.NotUsed
import akka.actor.Scheduler
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.HandlerRecoveryStrategy
import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.TestStatusObserver
import akka.projection.cassandra.internal.CassandraOffsetStore
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.scaladsl.ActorHandler
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.ProjectionManagement
import akka.projection.scaladsl.SourceProvider
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.projection.testkit.scaladsl.TestSourceProvider
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSource
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Promise

object CassandraProjectionSpec {
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

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 10.seconds, interval = 100.millis)

  private implicit val ec: ExecutionContext = system.executionContext
  private implicit val classicScheduler: Scheduler = system.classicSystem.scheduler
  private val offsetStore = new CassandraOffsetStore(system)
  private val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra.session-config")
  private val repository = new TestRepository(session)
  private val projectionTestKit = ProjectionTestKit(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // don't use futureValue (patience) here because it can take a while to start the test container
    Await.result(ContainerSessionProvider.started, 30.seconds)

    def tryCreateTable() =
      for {
        s <- session.underlying()
        // reason for setSchemaMetadataEnabled is that it speed up tests
        _ <- s.setSchemaMetadataEnabled(false).asScala
        _ <- offsetStore.createKeyspaceAndTable()
        _ <- repository.createKeyspaceAndTable()
        _ <- s.setSchemaMetadataEnabled(null).asScala
      } yield Done

    // the container can takes time to be 'ready',
    // we should keep trying to create the table until it succeeds
    Await.result(akka.pattern.retry(() => tryCreateTable(), 20, 3.seconds), 60.seconds)

  }

  override protected def afterAll(): Unit = {
    Await.ready(for {
      s <- session.underlying()
      // reason for setSchemaMetadataEnabled is that it speed up tests
      _ <- s.setSchemaMetadataEnabled(false).asScala
      _ <- session.executeDDL(s"DROP keyspace ${offsetStore.keyspace}")
      _ <- session.executeDDL(s"DROP keyspace ${repository.keyspace}")
      _ <- s.setSchemaMetadataEnabled(null).asScala
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

  private case class ImmediateFailOn4() extends Handler[Envelope] {

    @volatile var attempts = 0

    override def process(envelope: Envelope): Future[Done] = {
      if (envelope.offset == 4L) {
        attempts += 1
        throw TestException(concatHandlerFail4Msg + s" after $attempts attempts")
      } else {
        repository.concatToText(envelope.id, envelope.message)
      }
    }
  }

  private case class ControlledHandlerFailOn4(saw4Probe: ActorRef[Promise[Done]]) extends Handler[Envelope] {

    private var attempts = 0

    override def process(envelope: Envelope): Future[Done] = {
      if (envelope.offset == 4L) {
        attempts += 1
        val continue = Promise[Done]()
        // We can't just throw immediately here because we don't know if the downstream offset commit completed
        // yet, and most tests depend on that, instead, give the test a chance to observe we reached offset 4
        // do some assertions and then fail in a more deterministic fashion
        saw4Probe ! continue
        continue.future.map { _ =>
          throw TestException(concatHandlerFail4Msg + s" after $attempts attempts")
        }
      } else {
        repository.concatToText(envelope.id, envelope.message)
      }
    }
  }

  def offsetShouldBe(projectionId: ProjectionId, expectedOffset: Long) = {
    eventually {
      val currentOffset = offsetStore.readOffset[Long](projectionId).futureValue
      currentOffset shouldBe Some(expectedOffset)
    }
  }

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
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => concatHandler())
          .withSaveOffset(1, Duration.Zero)

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }
      offsetShouldBe(projectionId, 6L)
    }

    "restart from previous offset - handler throwing an exception, save after 1" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val failProbe = createTestProbe[Promise[Done]]()
      val failingProjection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](
            projectionId,
            sourceProvider(entityId),
            () => ControlledHandlerFailOn4(failProbe.ref))
          .withSaveOffset(1, Duration.Zero)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(failingProjection) { sinkProbe =>
          sinkProbe.request(1000)
          val fail = failProbe.receiveMessage() // handler saw 4
          offsetShouldBe(projectionId, 3L) // saving each offset, we should see offset 3 written
          fail.success(Done) // then we fail
          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        }
      }

      withClue("check: projection is consumed up to third") {
        val concatStr = repository.findById(entityId).futureValue
        concatStr should matchPattern {
          case Some(ConcatStr(_, "abc|def|ghi")) =>
        }
      }

      // re-run projection without failing function
      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => concatHandler())
          .withSaveOffset(1, Duration.Zero)

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }
      offsetShouldBe(projectionId, 6L)
    }

    "restart from previous offset - handler throwing an exception, save after 2" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val failProbe = createTestProbe[Promise[Done]]()
      val failingProjection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](
            projectionId,
            sourceProvider(entityId),
            () => ControlledHandlerFailOn4(failProbe.ref))
          .withSaveOffset(2, 1.minute)

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }
      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(failingProjection) { sinkProbe =>
          sinkProbe.request(1000)

          val fail = failProbe.receiveMessage() // handler saw 4
          offsetShouldBe(projectionId, 2L) // observe 2 committed
          fail.success(Done) // then fail

          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        }
      }

      eventually {
        withClue("check: projection is consumed up to third") {
          val concatStr = repository.findById(entityId).futureValue
          concatStr should matchPattern {
            case Some(ConcatStr(_, "abc|def|ghi")) =>
          }
        }
      }

      // re-run projection without failing function
      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => concatHandler())
          .withSaveOffset(2, 1.minute)

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          // note that 3rd is duplicated
          concatStr.text shouldBe "abc|def|ghi|ghi|jkl|mno|pqr"
        }
      }

      offsetShouldBe(projectionId, 6L)
    }

    "save offset after number of envelopes" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource[Envelope]()(system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, TestSourceProvider(source, _.offset), () => concatHandler())
          .withSaveOffset(10, 1.minute)

      projectionTestKit.runWithTestSink(projection) { sinkProbe =>
        eventually {
          sourceProbe.get should not be null
        }
        sinkProbe.request(1000)

        (1 to 15).foreach { n =>
          sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
        }
        offsetShouldBe(projectionId, 10L)
        eventually {
          repository.findById(entityId).futureValue.get.text should include("elem-15")
        }

        (16 to 22).foreach { n =>
          sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
        }
        offsetShouldBe(projectionId, 20L)
        eventually {
          repository.findById(entityId).futureValue.get.text should include("elem-22")
        }
      }
    }

    "save offset after idle duration" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource[Envelope]()(system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, TestSourceProvider(source, _.offset), () => concatHandler())
          .withSaveOffset(10, 2.seconds)

      projectionTestKit.runWithTestSink(projection) { sinkProbe =>

        eventually {
          sourceProbe.get should not be null
        }
        sinkProbe.request(1000)

        (1 to 15).foreach { n =>
          sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
        }
        offsetShouldBe(projectionId, 10L)
        eventually {
          repository.findById(entityId).futureValue.get.text should include("elem-15")
        }

        (16 to 17).foreach { n =>
          sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
        }
        offsetShouldBe(projectionId, 17L)
        repository.findById(entityId).futureValue.get.text should include("elem-17")

      }
    }

    "skip failing events when using RecoveryStrategy.skip" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => ImmediateFailOn4())
          .withSaveOffset(2, 1.minute)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      projectionTestKit.run(projection) {
        withClue("checking: all expected values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          // note that 4th is skipped
          concatStr.text shouldBe "abc|def|ghi|mno|pqr"
        }
      }

      offsetShouldBe(projectionId, 6L)
    }

    "skip failing events after retrying when using RecoveryStrategy.retryAndSkip" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handler = ImmediateFailOn4() // share the instance

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => handler)
          .withSaveOffset(2, 1.minute)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndSkip(3, 10.millis))
          .withStatusObserver(statusObserver)

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

      val someTestException = TestException("err")
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(entityId, 4, "jkl"), someTestException))
      statusProbe.expectNoMessage()

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue
        offset shouldBe Some(6L)
      }
    }

    "fail after retrying when using RecoveryStrategy.retryAndFail" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handler = ImmediateFailOn4()

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true)

      val projectionFailing =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => handler)
          .withSaveOffset(2, 1.minute)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(3, 10.millis))
          .withStatusObserver(statusObserver)

      spawn(ProjectionBehavior(projectionFailing))
      eventually {
        val concatStr = repository.findById(entityId).futureValue.get
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

    "verify offsets before processing an envelope" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val verifiedProbe: TestProbe[Long] = createTestProbe[Long]()

      val testVerification = (offset: Long) => {
        verifiedProbe.ref ! offset
        VerificationSuccess
      }

      def handler() = Handler[Envelope] { envelope =>
        withClue("checking: offset verified before handler function was run") {
          verifiedProbe.expectMessage(envelope.offset)
        }
        repository.concatToText(envelope.id, envelope.message)
      }

      val testSourceProvider = sourceProvider(entityId, verifyOffsetFn = testVerification)

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, testSourceProvider, () => handler())

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
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

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, testSourceProvider, () => concatHandler())

      projectionTestKit.run(projection) {
        withClue("checking: all values except skipped were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|jkl|mno|pqr" // `ghi` was skipped
        }
      }
    }
  }

  "A Cassandra grouped projection" must {

    "persist projection and offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      def groupedHandler(): Handler[immutable.Seq[Envelope]] = new Handler[immutable.Seq[Envelope]] {
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
          .groupedWithin[Long, Envelope](projectionId, sourceProvider(entityId), () => groupedHandler())
          .withGroup(groupAfterEnvelopes = 3, groupAfterDuration = 1.minute)

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }

      offsetShouldBe(projectionId, 6L)
    }
  }

  "A Cassandra flow projection" must {

    "persist projection and offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val flowHandler =
        FlowWithContext[Envelope, ProjectionContext]
          .mapAsync(1) { env =>
            repository.concatToText(env.id, env.message)
          }

      val projection =
        CassandraProjection
          .atLeastOnceFlow(projectionId, sourceProvider(entityId), flowHandler)
          .withSaveOffset(1, 1.minute)

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }

      offsetShouldBe(projectionId, 6L)
    }
  }

  "A Cassandra projection with actor handler" must {

    "start and stop actor" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val receiveProbe = createTestProbe[Envelope]()
      val stopProbe = createTestProbe[Done]()

      case class Req(envelope: Envelope, replyTo: ActorRef[Done])

      val behavior: Behavior[Req] =
        Behaviors
          .receiveMessage[Req] {
            case Req(env, replyTo) =>
              receiveProbe.ref ! env
              replyTo ! Done
              Behaviors.same
          }
          .receiveSignal {
            case (_, PostStop) =>
              stopProbe.ref ! Done
              Behaviors.same
          }

      def actorHandler(): Handler[Envelope] = new ActorHandler[Envelope, Req](behavior) {
        import akka.actor.typed.scaladsl.AskPattern._

        override def process(actor: ActorRef[Req], envelope: Envelope): Future[Done] = {
          actor.ask[Done](replyTo => Req(envelope, replyTo))
        }
      }

      val projection =
        CassandraProjection
          .atLeastOnce(projectionId, sourceProvider(entityId), () => actorHandler())
          .withSaveOffset(1, 1.minute)

      val projectionRef = spawn(ProjectionBehavior(projection))

      receiveProbe.receiveMessage().message shouldBe "abc"
      receiveProbe.receiveMessage().message shouldBe "def"
      receiveProbe.receiveMessage().message shouldBe "ghi"
      receiveProbe.receiveMessage().message shouldBe "jkl"
      receiveProbe.receiveMessage().message shouldBe "mno"
      receiveProbe.receiveMessage().message shouldBe "pqr"

      projectionRef ! ProjectionBehavior.Stop

      stopProbe.receiveMessage()

      offsetShouldBe(projectionId, 6L)
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
          .atMostOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => concatHandler())

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }

      offsetShouldBe(projectionId, 6L)
    }

    "restart from next offset - handler throwing an exception" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val failProbe = createTestProbe[Promise[Done]]()
      val failingProjection =
        CassandraProjection
          .atMostOnce[Long, Envelope](
            projectionId,
            sourceProvider(entityId),
            () => ControlledHandlerFailOn4(failProbe.ref))

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        projectionTestKit.runWithTestSink(failingProjection) { sinkProbe =>
          sinkProbe.request(1000)
          failProbe.receiveMessage().success(Done)
          eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
        }
      }

      offsetShouldBe(projectionId, 4L)
      withClue("check: projection is consumed up to third") {
        val concatStr = repository.findById(entityId).futureValue
        concatStr should matchPattern {
          case Some(ConcatStr(_, "abc|def|ghi")) =>
        }
      }

      // re-run projection without failing function
      val projection =
        CassandraProjection
          .atMostOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => concatHandler())

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          // failed: jkl not included
          concatStr.text shouldBe "abc|def|ghi|mno|pqr"
        }
      }

      offsetShouldBe(projectionId, 6L)
    }
  }

  "CassandraProjection lifecycle" must {

    class LifecycleHandler(
        probe: ActorRef[String],
        failOnceOnOffset: AtomicInteger = new AtomicInteger(-1),
        alwaysFailOnOffset: Int = -1)
        extends Handler[Envelope] {

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

      override def process(envelope: Envelope): Future[Done] = {
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
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val handlerProbe = createTestProbe[String]()
      val handler = new LifecycleHandler(handlerProbe.ref)

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true)

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => handler)
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
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => handler)
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
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), handlerFactory)
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
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => handler)
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
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId), () => handler)
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

  "CassandraProjection management" must {
    "restart from beginning when offset is cleared" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId, complete = false), () => concatHandler())
          .withSaveOffset(1, Duration.Zero)

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

      val concatStr1 = repository.findById(entityId).futureValue.get
      concatStr1.text shouldBe "abc|def|ghi|jkl|mno|pqr"

      ProjectionManagement(system).clearOffset(projectionId).futureValue shouldBe Done
      eventually {
        val concatStr = repository.findById(entityId).futureValue.get
        concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr|abc|def|ghi|jkl|mno|pqr"
      }
    }

    "restart from updated offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId, complete = false), () => concatHandler())
          .withSaveOffset(1, Duration.Zero)

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

      val concatStr1 = repository.findById(entityId).futureValue.get
      concatStr1.text shouldBe "abc|def|ghi|jkl|mno|pqr"

      ProjectionManagement(system).updateOffset(projectionId, 3L).futureValue shouldBe Done
      eventually {
        val concatStr = repository.findById(entityId).futureValue.get
        concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr|jkl|mno|pqr"
      }
    }

    "pause projection" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](projectionId, sourceProvider(entityId, complete = false), () => concatHandler())
          .withSaveOffset(1, Duration.Zero)

      // not using ProjectionTestKit because want to test ProjectionManagement
      spawn(ProjectionBehavior(projection))

      val mgmt = ProjectionManagement(system)

      mgmt.isPaused(projectionId).futureValue shouldBe false

      eventually {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(6L)
      }

      ProjectionManagement(system).getOffset(projectionId).futureValue shouldBe Some(6L)

      repository.findById(entityId).futureValue.get.text shouldBe "abc|def|ghi|jkl|mno|pqr"

      mgmt.pause(projectionId).futureValue shouldBe Done
      mgmt.clearOffset(projectionId).futureValue shouldBe Done

      mgmt.isPaused(projectionId).futureValue shouldBe true

      Thread.sleep(500)
      // not updated because paused
      repository.findById(entityId).futureValue.get.text shouldBe "abc|def|ghi|jkl|mno|pqr"

      mgmt.resume(projectionId)

      mgmt.isPaused(projectionId).futureValue shouldBe false

      eventually {
        repository.findById(entityId).futureValue.get.text shouldBe "abc|def|ghi|jkl|mno|pqr|abc|def|ghi|jkl|mno|pqr"
      }
    }
  }
}
