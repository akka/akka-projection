/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.immutable
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
import akka.persistence.r2dbc.query.TimestampOffset
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
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
import akka.stream.testkit.TestSubscriber
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object R2dbcTimestampOffsetProjectionSpec {

  def sourceProvider(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      complete: Boolean = true): SourceProvider[TimestampOffset, EventEnvelope[String]] = {
    val sp = TestSourceProvider[TimestampOffset, EventEnvelope[String]](
      Source(envelopes),
      _.offset.asInstanceOf[TimestampOffset])
      .withStartSourceFrom { (lastProcessedOffset, offset) =>
        offset.timestamp.isBefore(lastProcessedOffset.timestamp) ||
        (offset.timestamp == lastProcessedOffset.timestamp && offset.seen == lastProcessedOffset.seen)
      }
    if (complete) sp.withAllowCompletion(true)
    else sp
  }

  def backtrackingSourceProvider(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      complete: Boolean = true): SourceProvider[TimestampOffset, EventEnvelope[String]] = {
    val sp = TestSourceProvider[TimestampOffset, EventEnvelope[String]](
      Source(envelopes),
      _.offset.asInstanceOf[TimestampOffset])
      .withStartSourceFrom { (lastProcessedOffset, offset) => false } // include all
    if (complete) sp.withAllowCompletion(true)
    else sp
  }

}

class R2dbcTimestampOffsetProjectionSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import R2dbcOffsetStore.Pid
  import R2dbcOffsetStore.SeqNr
  import R2dbcProjectionSpec.ConcatStr
  import R2dbcProjectionSpec.TestRepository
  import R2dbcTimestampOffsetProjectionSpec._

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext

  private val logger = LoggerFactory.getLogger(getClass)
  private val settings = R2dbcProjectionSettings(testKit.system)
  private def createOffsetStore(projectionId: ProjectionId): R2dbcOffsetStore =
    new R2dbcOffsetStore(projectionId, system, settings, r2dbcExecutor)
  private val projectionTestKit = ProjectionTestKit(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // FIXME create table doesn't work from here (timeout) and therefore workaround by
    // adding to create_tables_postgres.sql
    //    Await.result(
    //      r2dbcExecutor.withConnection("beforeAll createTable") { conn =>
    //        TestRepository(session).createTable()
    //      },
    //      10.seconds)
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(_.createStatement(s"delete from ${TestRepository.table}")),
      10.seconds)
  }

  private def offsetShouldBe[Offset](expected: Offset)(implicit offsetStore: R2dbcOffsetStore) = {
    val offset = offsetStore.readOffset[TimestampOffset]().futureValue
    offset shouldBe Some(expected.asInstanceOf[TimestampOffset].copy(readTimestamp = Instant.EPOCH))
  }

  private def offsetShouldBeEmpty()(implicit offsetStore: R2dbcOffsetStore) = {
    offsetStore.readOffset[TimestampOffset]().futureValue shouldBe empty
  }

  private def projectedValueShouldBe(expected: String)(implicit entityId: String) = {
    val opt = withRepo(_.findById(entityId)).futureValue.map(_.text)
    opt shouldBe Some(expected)
  }

  private def projectedValueShouldBeEmpty()(implicit entityId: String) = {
    val opt = withRepo(_.findById(entityId)).futureValue.map(_.text)
    opt shouldBe None
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

  class ConcatHandler(failPredicate: EventEnvelope[String] => Boolean = _ => false)
      extends R2dbcHandler[EventEnvelope[String]] {

    private val logger = LoggerFactory.getLogger(getClass)
    private val _attempts = new AtomicInteger()
    def attempts: Int = _attempts.get

    override def process(session: R2dbcSession, envelope: EventEnvelope[String]): Future[Done] = {
      if (failPredicate(envelope)) {
        _attempts.incrementAndGet()
        throw TestException(concatHandlerFail4Msg + s" after $attempts attempts")
      } else {
        logger.debug(s"handling {}", envToString(envelope))
        TestRepository(session).concatToText(envelope.persistenceId, envelope.event)
      }
    }

    // TODO add toString to EventEnvelope
    def envToString[Envelope](envelope: Envelope): AnyRef = new AnyRef {
      override def toString: String = envelope match {
        case env: EventEnvelope[_] =>
          s"EventEnvelope(${env.offset}, ${env.persistenceId}, ${env.sequenceNr})"
        case env => env.toString
      }
    }
  }

  private val clock = new TestClock
  def tick(): TestClock = {
    clock.tick(JDuration.ofMillis(1))
    clock
  }

  def createEnvelope(pid: Pid, seqNr: SeqNr, timestamp: Instant, event: String): EventEnvelope[String] =
    createEnvelope(pid, seqNr, timestamp, readAfterMillis = 10, event)

  def createEnvelope(
      pid: Pid,
      seqNr: SeqNr,
      timestamp: Instant,
      readAfterMillis: Int,
      event: String): EventEnvelope[String] =
    EventEnvelope(
      TimestampOffset(timestamp, timestamp.plusMillis(readAfterMillis), Map(pid -> seqNr)),
      pid,
      seqNr,
      event,
      timestamp.toEpochMilli)

  def createEnvelopes(pid: Pid, numberOfEvents: Int): immutable.IndexedSeq[EventEnvelope[String]] = {
    (1 to numberOfEvents).map { n =>
      createEnvelope(pid, n, tick().instant(), s"e$n")
    }
  }

  def createEnvelopesWithDuplicates(pid1: Pid, pid2: Pid): Vector[EventEnvelope[String]] = {
    val startTime = Instant.now()
    Vector(
      createEnvelope(pid1, 1, startTime, s"e1-1"),
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      createEnvelope(pid2, 1, startTime.plusMillis(2), s"e2-1"),
      createEnvelope(pid1, 3, startTime.plusMillis(4), s"e1-3"),
      // pid1-3 is emitted before pid2-2 even though pid2-2 timestamp is earlier,
      // from backtracking query previous events are emitted again, including the missing pid2-2
      createEnvelope(pid1, 1, startTime, s"e1-1"),
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      createEnvelope(pid2, 1, startTime.plusMillis(2), s"e2-1"),
      // now it pid2-2 is included
      createEnvelope(pid2, 2, startTime.plusMillis(3), s"e2-2"),
      createEnvelope(pid1, 3, startTime.plusMillis(4), s"e1-3"),
      // and then some normal again
      createEnvelope(pid1, 4, startTime.plusMillis(5), s"e1-4"),
      createEnvelope(pid2, 3, startTime.plusMillis(6), s"e2-3"))
  }

  def createEnvelopesUnknownSequenceNumbers(startTime: Instant, pid1: Pid, pid2: Pid): Vector[EventEnvelope[String]] = {
    Vector(
      createEnvelope(pid1, 1, startTime, s"e1-1"),
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      // first pid2 seqNr is not 1, will be rejected
      createEnvelope(pid2, 3, startTime.plusMillis(2), s"e2-3"),
      createEnvelope(pid1, 3, startTime.plusMillis(4), s"e1-3"),
      // pid2 seqNr 4 missing, will reject 5
      createEnvelope(pid1, 5, startTime.plusMillis(6), s"e1-5"))
  }

  def createEnvelopesBacktrackingUnknownSequenceNumbers(
      startTime: Instant,
      pid1: Pid,
      pid2: Pid): Vector[EventEnvelope[String]] = {
    Vector(
      // may also contain some duplicates
      createEnvelope(pid1, 2, startTime.plusMillis(1), readAfterMillis = 10000, s"e1-2"),
      createEnvelope(pid2, 3, startTime.plusMillis(2), readAfterMillis = 10000, s"e2-3"),
      createEnvelope(pid1, 3, startTime.plusMillis(4), readAfterMillis = 10000, s"e1-3"),
      createEnvelope(pid1, 4, startTime.plusMillis(5), readAfterMillis = 10000, s"e1-4"),
      createEnvelope(pid1, 5, startTime.plusMillis(6), readAfterMillis = 10000, s"e1-5"),
      createEnvelope(pid2, 4, startTime.plusMillis(7), readAfterMillis = 10000, s"e2-4"),
      createEnvelope(pid1, 6, startTime.plusMillis(8), readAfterMillis = 10000, s"e1-6"))
  }

  def groupedHandler(probe: ActorRef[String]): R2dbcHandler[Seq[EventEnvelope[String]]] = {
    R2dbcHandler[immutable.Seq[EventEnvelope[String]]] { (session, envelopes) =>
      probe ! "called"
      if (envelopes.isEmpty)
        Future.successful(Done)
      else {
        val repo = TestRepository(session)
        val results = envelopes.groupBy(_.persistenceId).map { case (pid, envs) =>
          repo.findById(pid).flatMap { existing =>
            val newConcatStr = envs.foldLeft(existing.getOrElse(ConcatStr(pid, ""))) { (acc, env) =>
              acc.concat(env.event)
            }
            repo.update(pid, newConcatStr.text)
          }
        }
        Future.sequence(results).map(_ => Done)
      }
    }
  }

  "A R2DBC exactly-once projection with TimestampOffset" must {

    "persist projection and offset in the same write operation (transactional)" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val envelopes = createEnvelopes(pid, 6)

      val projection =
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(envelopes),
          handler = () => new ConcatHandler)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(envelopes.last.offset)
    }

    "restart from previous offset - fail with throwing an exception" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val envelopes = createEnvelopes(pid, 6)

      def exactlyOnceProjection(failWhen: EventEnvelope[String] => Boolean = _ => false) = {
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(envelopes),
          handler = () => new ConcatHandler(failWhen))
      }

      offsetShouldBeEmpty()
      projectionTestKit.runWithTestSink(exactlyOnceProjection(_.sequenceNr == 4)) { sinkProbe =>
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
      }
      projectedValueShouldBe("e1|e2|e3")
      offsetShouldBe(envelopes(2).offset)

      // re-run projection without failing function
      projectionTestKit.run(exactlyOnceProjection()) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(envelopes.last.offset)
    }

    "filter out duplicates" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)

      val projection =
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(envelopes),
          handler = () => new ConcatHandler)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4")(pid1)
        projectedValueShouldBe("e2-1|e2-2|e2-3")(pid2)
      }
      offsetShouldBe(envelopes.last.offset)
    }

    "filter out unknown sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val startTime = Instant.now()

      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      val projection1 =
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(envelopes1),
          handler = () => new ConcatHandler)

      projectionTestKit.run(projection1) {
        projectedValueShouldBe("e1-1|e1-2|e1-3")(pid1)
        projectedValueShouldBeEmpty()(pid2)
      }
      offsetShouldBe(envelopes1.collectFirst { case env if env.event == "e1-3" => env.offset }.get)

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      val projection2 =
        R2dbcProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = backtrackingSourceProvider(envelopes2),
          handler = () => new ConcatHandler)

      projectionTestKit.run(projection2) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4|e1-5|e1-6")(pid1)
        projectedValueShouldBe("e2-3|e2-4")(pid2)
      }
      offsetShouldBe(envelopes2.last.offset)
    }

  }

  "A R2DBC grouped projection with TimestampOffset" must {
    "persist projection and offset in the same write operation (transactional)" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val handlerProbe = createTestProbe[String]("calls-to-handler")

      val envelopes = createEnvelopes(pid, 6)

      val projection =
        R2dbcProjection
          .groupedWithin(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(envelopes),
            handler = () => groupedHandler(handlerProbe.ref))
          .withGroup(3, 3.seconds)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }

      // handler probe is called twice
      handlerProbe.expectMessage("called")
      handlerProbe.expectMessage("called")

      offsetShouldBe(envelopes.last.offset)
    }

    "filter duplicates" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val handlerProbe = createTestProbe[String]("calls-to-handler")

      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)

      val projection =
        R2dbcProjection
          .groupedWithin(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(envelopes),
            handler = () => groupedHandler(handlerProbe.ref))
          .withGroup(3, 3.seconds)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4")(pid1)
        projectedValueShouldBe("e2-1|e2-2|e2-3")(pid2)
      }

      // handler probe is called twice
      handlerProbe.expectMessage("called")
      handlerProbe.expectMessage("called")

      offsetShouldBe(envelopes.last.offset)
    }

    "filter out unknown sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val handlerProbe = createTestProbe[String]()

      val startTime = Instant.now()

      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      val projection1 =
        R2dbcProjection
          .groupedWithin(
            projectionId,
            Some(settings),
            sourceProvider = backtrackingSourceProvider(envelopes1),
            handler = () => groupedHandler(handlerProbe.ref))
          .withGroup(3, 3.seconds)

      projectionTestKit.run(projection1) {
        projectedValueShouldBe("e1-1|e1-2|e1-3")(pid1)
        projectedValueShouldBeEmpty()(pid2)
      }
      offsetShouldBe(envelopes1.collectFirst { case env if env.event == "e1-3" => env.offset }.get)

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      val projection2 =
        R2dbcProjection
          .groupedWithin(
            projectionId,
            Some(settings),
            sourceProvider = backtrackingSourceProvider(envelopes2),
            handler = () => groupedHandler(handlerProbe.ref))
          .withGroup(3, 3.seconds)

      projectionTestKit.run(projection2) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4|e1-5|e1-6")(pid1)
        projectedValueShouldBe("e2-3|e2-4")(pid2)
      }
      offsetShouldBe(envelopes2.last.offset)
    }

    "handle grouped async projection" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val envelopes = createEnvelopes(pid, 6)

      val result = new StringBuffer()

      def handler(): Handler[immutable.Seq[EventEnvelope[String]]] = new Handler[immutable.Seq[EventEnvelope[String]]] {
        override def process(envelopes: immutable.Seq[EventEnvelope[String]]): Future[Done] = {
          Future {
            envelopes.foreach(env => result.append(env.event).append("|"))
          }.map(_ => Done)
        }
      }

      val projection =
        R2dbcProjection
          .groupedWithinAsync(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(envelopes),
            handler = () => handler())
          .withGroup(2, 3.seconds)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        result.toString shouldBe "e1|e2|e3|e4|e5|e6|"
      }

      // FIXME groupedWithinAsync doesn't filter/store offsets correctly yet
      pending
      //offsetShouldBe(envelopes.last.offset)
    }

    "filter duplicates for grouped async projection" in {
      pending // FIXME
    }

    "filter out unknown sequence numbers for grouped async projection" in {
      pending // FIXME see corresponding test for duplicates and above exactly-once
    }
  }

  "A R2DBC at-least-once projection with TimestampOffset" must {

    "persist projection and offset" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val envelopes = createEnvelopes(pid, 6)

      val projection =
        R2dbcProjection.atLeastOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(envelopes),
          handler = () => new ConcatHandler)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(envelopes.last.offset)
    }

    "filter duplicates" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)

      val projection =
        R2dbcProjection.atLeastOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(envelopes),
          handler = () => new ConcatHandler)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4")(pid1)
        projectedValueShouldBe("e2-1|e2-2|e2-3")(pid2)
      }
      offsetShouldBe(envelopes.last.offset)
    }

    "filter out unknown sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val startTime = Instant.now()

      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      val projection1 =
        R2dbcProjection.atLeastOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(envelopes1),
          handler = () => new ConcatHandler)

      projectionTestKit.run(projection1) {
        projectedValueShouldBe("e1-1|e1-2|e1-3")(pid1)
        projectedValueShouldBeEmpty()(pid2)
      }
      offsetShouldBe(envelopes1.collectFirst { case env if env.event == "e1-3" => env.offset }.get)

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      val projection2 =
        R2dbcProjection.atLeastOnce(
          projectionId,
          Some(settings),
          sourceProvider = backtrackingSourceProvider(envelopes2),
          handler = () => new ConcatHandler)

      projectionTestKit.run(projection2) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4|e1-5|e1-6")(pid1)
        projectedValueShouldBe("e2-3|e2-4")(pid2)
      }
      offsetShouldBe(envelopes2.last.offset)
    }

    "handle async projection" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val result = new StringBuffer()

      def handler(): Handler[EventEnvelope[String]] = new Handler[EventEnvelope[String]] {
        override def process(envelope: EventEnvelope[String]): Future[Done] = {
          Future {
            result.append(envelope.event).append("|")
          }.map(_ => Done)
        }
      }

      val envelopes = createEnvelopes(pid, 6)

      val projection =
        R2dbcProjection.atLeastOnceAsync(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider(envelopes),
          handler = () => handler())

      projectionTestKit.run(projection) {
        result.toString shouldBe "e1|e2|e3|e4|e5|e6|"
      }
      offsetShouldBe(envelopes.last.offset)
    }

    "filter duplicates for async projection" in {
      pending // FIXME
    }

    "filter out unknown sequence numbers for async projection" in {
      pending // FIXME
    }
  }

  "A R2DBC flow projection with TimestampOffset" must {

    "persist projection and offset" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      offsetShouldBeEmpty()

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            withRepo(_.concatToText(env.persistenceId, env.event))
          }

      val envelopes = createEnvelopes(pid, 6)

      val projection =
        R2dbcProjection
          .atLeastOnceFlow(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(envelopes),
            handler = flowHandler)
          .withSaveOffset(1, 1.minute)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      offsetShouldBe(envelopes.last.offset)
    }

    "filter duplicates" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            withRepo(_.concatToText(env.persistenceId, env.event))
          }

      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)
      val projection =
        R2dbcProjection
          .atLeastOnceFlow(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(envelopes),
            handler = flowHandler)
          .withSaveOffset(2, 1.minute)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4")(pid1)
        projectedValueShouldBe("e2-1|e2-2|e2-3")(pid2)
      }
      offsetShouldBe(envelopes.last.offset)
    }

    "filter out unknown sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            withRepo(_.concatToText(env.persistenceId, env.event))
          }

      val startTime = Instant.now()

      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      val projection1 =
        R2dbcProjection
          .atLeastOnceFlow(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(envelopes1),
            handler = flowHandler)
          .withSaveOffset(2, 1.minute)

      projectionTestKit.run(projection1) {
        projectedValueShouldBe("e1-1|e1-2|e1-3")(pid1)
        projectedValueShouldBeEmpty()(pid2)
      }
      offsetShouldBe(envelopes1.collectFirst { case env if env.event == "e1-3" => env.offset }.get)

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      val projection2 =
        R2dbcProjection
          .atLeastOnceFlow(
            projectionId,
            Some(settings),
            sourceProvider = backtrackingSourceProvider(envelopes2),
            handler = flowHandler)
          .withSaveOffset(2, 1.minute)

      projectionTestKit.run(projection2) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4|e1-5|e1-6")(pid1)
        projectedValueShouldBe("e2-3|e2-4")(pid2)
      }
      offsetShouldBe(envelopes2.last.offset)
    }
  }

  "R2dbcProjection management with TimestampOffset" must {

    "restart from beginning when offset is cleared" in {
      pending // FIXME not implemented yet
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val envelopes = createEnvelopes(pid, 6)

      val projection =
        R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(envelopes),
            handler = () =>
              R2dbcHandler[EventEnvelope[String]] { (session, envelope) =>
                TestRepository(session).concatToText(envelope.persistenceId, envelope.event)
              })

      offsetShouldBeEmpty()

      // not using ProjectionTestKit because want to test ProjectionManagement
      spawn(ProjectionBehavior(projection))
      eventually {
        offsetShouldBe(envelopes.last.offset)
      }
      ProjectionManagement(system).getOffset(projectionId).futureValue shouldBe Some(envelopes.last.offset)
      projectedValueShouldBe("e1|e2|e3|e4|e5|e6")

      ProjectionManagement(system).clearOffset(projectionId).futureValue shouldBe Done
      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6|e1|e2|e3|e4|e5|e6")
      }
    }

    "restart from updated offset" in {
      pending // FIXME not implemented yet
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      implicit val offsetStore = createOffsetStore(projectionId)

      val envelopes = createEnvelopes(pid, 6)

      val projection =
        R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider(envelopes),
            handler = () =>
              R2dbcHandler[EventEnvelope[String]] { (session, envelope) =>
                TestRepository(session).concatToText(envelope.persistenceId, envelope.event)
              })

      offsetShouldBeEmpty()

      // not using ProjectionTestKit because want to test ProjectionManagement
      spawn(ProjectionBehavior(projection))

      eventually {
        offsetShouldBe(envelopes.last.offset)
      }
      ProjectionManagement(system).getOffset(projectionId).futureValue shouldBe Some(envelopes.last.offset)
      projectedValueShouldBe("e1|e2|e3|e4|e5|e6")

      ProjectionManagement(system).updateOffset(projectionId, envelopes(2).offset).futureValue shouldBe Done
      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6|e4|e5|e6")
      }
    }

  }
}
