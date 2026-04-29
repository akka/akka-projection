/*
 * Copyright (C) 2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.dynamodb.internal

import java.time.Clock
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.persistence.Persistence
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.typed.PersistenceId
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Record
import akka.projection.dynamodb.internal.DynamoDBOffsetStoreSpec.LoadSequenceNumber
import akka.projection.dynamodb.internal.DynamoDBOffsetStoreSpec.ProbeStubOffsetStoreDao
import akka.projection.eventsourced.scaladsl.EventSourcedProvider.LoadEventsByPersistenceIdSourceProvider
import akka.projection.internal.ProjectionContextImpl
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.Assertion
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.RejectedEnvelope

object DynamoDBAtLeastOnceFlowSpec {
  val config = {
    val default = ConfigFactory.load()

    ConfigFactory.parseString("""
      akka.loglevel = DEBUG
      akka.projection.dynamodb.max-concurrent-validations = 3
    """).withFallback(default)
  }

  final case class ResolveSource(
      offset: () => Future[Option[TimestampOffset]],
      promise: Promise[Source[EventEnvelope[String], NotUsed]])

  sealed trait ByPidCommand

  final case class TimestampOf(sequenceNr: Long, promise: Promise[Option[Instant]]) extends ByPidCommand
  final case class LoadEnvelope[Event](sequenceNr: Long, promise: Promise[EventEnvelope[Event]]) extends ByPidCommand

  final case class CurrentEventsByPersistenceId(
      from: Long,
      to: Long,
      promise: Promise[Source[EventEnvelope[String], NotUsed]])
      extends ByPidCommand

  class ProbeStubSourceProvider(testKit: ActorTestKit)
      extends SourceProvider[TimestampOffset, EventEnvelope[String]]
      with BySlicesSourceProvider
      with EventTimestampQuery
      with LoadEventQuery
      with LoadEventsByPersistenceIdSourceProvider[String] {
    import testKit.system

    val persistenceExt = Persistence(testKit.system)

    val sourceProbe = TestProbe[ResolveSource]()
    val probesByPid = new ConcurrentHashMap[String, TestProbe[ByPidCommand]]()

    def initializeProbes(pid: String): Unit = {
      probesByPid.putIfAbsent(pid, TestProbe())
    }

    override def source(
        offset: () => Future[Option[TimestampOffset]]): Future[Source[EventEnvelope[String], NotUsed]] = {
      val reified = ResolveSource(offset, Promise())
      sourceProbe.ref ! reified
      reified.promise.future
    }

    override def extractOffset(envelope: EventEnvelope[String]): TimestampOffset =
      envelope.offset.asInstanceOf[TimestampOffset]
    override def extractCreationTime(envelope: EventEnvelope[String]): Long = envelope.timestamp
    override def minSlice: Int = 0
    override def maxSlice: Int = persistenceExt.numberOfSlices - 1

    override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
      val reified = TimestampOf(sequenceNr, Promise())
      initializeProbes(persistenceId)
      probesByPid.get(persistenceId).ref ! reified
      reified.promise.future
    }

    override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] = {
      val reified = LoadEnvelope[Event](sequenceNr, Promise())
      initializeProbes(persistenceId)
      probesByPid.get(persistenceId).ref ! reified
      reified.promise.future
    }

    private[akka] def currentEventsByPersistenceId(
        persistenceId: String,
        fromSequenceNr: Long,
        toSequenceNr: Long): Option[Source[EventEnvelope[String], NotUsed]] = {
      Some(
        Source
          .futureSource(stubCEP(persistenceId, fromSequenceNr, toSequenceNr).future)
          .mapMaterializedValue(_ => NotUsed))
    }

    final protected def stubCEP(pid: String, from: Long, to: Long): Promise[Source[EventEnvelope[String], NotUsed]] = {
      val reified = CurrentEventsByPersistenceId(from, to, Promise())
      initializeProbes(pid)
      probesByPid.get(pid).ref ! reified
      reified.promise
    }
  }

  val baseSettings = DynamoDBOffsetStoreSpec.baseSettings.withMaxConcurrentValidations(3)

  val noOpHandler = FlowWithContext[EventEnvelope[String], ProjectionContext].map { _ => Done }

  val projectionId = ProjectionId.of("some-projection", "some-key")
  def uuid() = UUID.randomUUID().toString

  private def createOffsetStore(testKit: ActorTestKit, clock: Clock, sourceProvider: Option[BySlicesSourceProvider]) = {
    val dao = new ProbeStubOffsetStoreDao(testKit)
    dao -> new DynamoDBOffsetStore(projectionId, uuid(), sourceProvider, testKit.system, baseSettings, dao, clock)
  }

  val FutureNone = Future.successful(None)
}

//@annotation.nowarn("msg=never used")
class DynamoDBAtLeastOnceFlowSpec
    extends ScalaTestWithActorTestKit(DynamoDBAtLeastOnceFlowSpec.config)
    with AnyWordSpecLike
    with LogCapturing {
  import DynamoDBAtLeastOnceFlowSpec._

  def initializeProbes(
      pids: IterableOnce[String],
      sourceProvider: ProbeStubSourceProvider,
      dao: ProbeStubOffsetStoreDao): Unit = {
    pids.iterator.foreach { pid =>
      val slice = sourceProvider.persistenceExt.sliceForPersistenceId(pid)
      sourceProvider.initializeProbes(pid)
      dao.initializeProbes(Some(pid), Some(slice))
    }
  }

  private def mappedSource(
      sourceProvider: SourceProvider[TimestampOffset, EventEnvelope[String]],
      offsetStore: DynamoDBOffsetStore,
      readOffsets: Future[Option[TimestampOffset]]): Source[(Done, ProjectionContext), NotUsed] = {
    val handler =
      DynamoDBProjectionImpl.adaptedHandlerForFlow(sourceProvider, noOpHandler, offsetStore, baseSettings)

    Source
      .futureSource(sourceProvider.source(() => readOffsets))
      .map { env =>
        env -> ProjectionContextImpl(sourceProvider.extractOffset(env), env, offsetStore.uuid)
      }
      .via(handler.asFlow)
      .mapMaterializedValue(_ => NotUsed)
  }

  def createEnvelope(
      pid: String,
      seqNr: Long,
      timestamp: Instant,
      event: String,
      clock: Clock): EventEnvelope[String] = {
    val entityType = PersistenceId.extractEntityType(pid)
    val slice = Persistence(system).sliceForPersistenceId(pid)

    EventEnvelope(
      TimestampOffset(timestamp, clock.instant(), Map(pid -> seqNr)),
      pid,
      seqNr,
      event,
      timestamp.toEpochMilli,
      entityType,
      slice,
      filtered = false,
      source = EnvelopeOrigin.SourceQuery)
  }

  def backtracking(env: EventEnvelope[String]): EventEnvelope[String] =
    EventEnvelope(
      env.offset,
      env.persistenceId,
      env.sequenceNr,
      null,
      env.timestamp,
      env.entityType,
      env.slice,
      env.filtered,
      source = EnvelopeOrigin.SourceBacktracking)

  trait ProjectionContextExtractor[Result] {
    def unapply(ctx: ProjectionContext): Option[Result]
  }

  val FromProjectionContext = new ProjectionContextExtractor[String] {
    def unapply(ctx: ProjectionContext): Option[String] =
      ctx match {
        case ProjectionContextImpl(offset: TimestampOffset, env: EventEnvelope[_], _, _, _, _) =>
          env.eventOption.flatMap {
            _ match {
              case s: String => Some(s)
              case _         => None
            }
          }

        case _ => None
      }
  }

  private def withHarness(
      test: (Clock, ProbeStubSourceProvider, ProbeStubOffsetStoreDao, DynamoDBOffsetStore) => Assertion): Assertion = {
    val clock = Clock.systemUTC()
    val sourceProvider = new ProbeStubSourceProvider(testKit)
    val (dao, offsetStore) = createOffsetStore(testKit, clock, Some(sourceProvider))
    test(clock, sourceProvider, dao, offsetStore)
  }

  "The adapted handler for flow projections" should {
    Seq(true, false).foreach { p1First =>
      s"concurrently validate envelopes for disjoint pids (p1First = $p1First)" in withHarness {
        (clock, sourceProvider, dao, offsetStore) =>
          val startTime = clock.instant()

          initializeProbes(Set("p1", "p2", "p3"), sourceProvider, dao)
          val source = mappedSource(sourceProvider, offsetStore, FutureNone)
          val sinkProbe = source.runWith(TestSink())
          val (eventProbe, eventSource) = TestSource[EventEnvelope[String]]().preMaterialize()
          sourceProvider.sourceProbe.receiveMessage().promise.success(eventSource)
          eventProbe.ensureSubscription()
          sinkProbe.request(1)

          baseSettings.maxConcurrentValidations shouldBe 3
          eventProbe.sendNext(createEnvelope("p1", 1, startTime.minusMillis(1000), "e1", clock))
          val p1Load = dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber]
          // no wait, mapAsyncPartitioned buffer empties
          eventProbe.sendNext(createEnvelope("p1", 2, startTime.minusMillis(950), "e2", clock))
          dao.probesByPid.get("p1").expectNoMessage(30.millis)
          // waiting on e1 to be in-flight... mapAsyncPartitoned buffer is 1/3
          val e3 = createEnvelope("p2", 1, startTime.minusMillis(917), "e3", clock)
          eventProbe.sendNext(e3)
          val p2Load = dao.probesByPid.get("p2").expectMessageType[LoadSequenceNumber]
          // future complete, but buffered behind e2, mapAsyncPartitioned buffer is 2/3
          eventProbe.sendNext(createEnvelope("p1", 3, startTime.minusMillis(920), "e4", clock))
          // waiting on e2's validation future, mapAsyncPartitioned buffer is 3/3, but a fourth element is demanded

          if (p1First) {
            p1Load.promise.success(None) // seqNr is 1, so accept
            // e1 goes in-flight, unblocks e2 which validates immediately and emits
            dao.probesByPid.get("p1").expectNoMessage(30.millis)
            eventProbe.sendNext(createEnvelope("p3", 2, startTime.minusMillis(921), "e5", clock))
            sinkProbe.requestNext()._2 match {
              case FromProjectionContext(event) => event shouldBe "e1"
              case _                            => fail("Unknown context type")
            }
            // e2's emission starts validation on e5 (note that e3 is still pending validation)
            dao.probesByPid
              .get("p3")
              .expectMessageType[LoadSequenceNumber]
              .promise
              .success(
                Some(
                  Record(
                    sourceProvider.persistenceExt.sliceForPersistenceId("p3"),
                    "p3",
                    1,
                    startTime.minusSeconds(86200))))

            sinkProbe.requestNext()._2 match {
              case FromProjectionContext(event) => event shouldBe "e2"
              case _                            => fail("Unknown context type")
            }
            // e3 validation still incomplete
            sinkProbe.request(1)
            assert(eventProbe.pending > 0, "there should be unfulfilled demand on the source")
            sinkProbe.expectNoMessage(30.millis)

            p2Load.promise.success(None)
            sinkProbe.requestNext()._2 match {
              case FromProjectionContext(event) => event shouldBe "e3"
              case _                            => fail("Unknown context type")
            }

            sinkProbe.requestNext()._2 match {
              case FromProjectionContext(event) => event shouldBe "e4"
              case _                            => fail("Unknown context type")
            }

            sinkProbe.requestNext()._2 match {
              case FromProjectionContext(event) => event shouldBe "e5"
              case _                            => fail("Unknown context type")
            }
          } else {
            p2Load.promise.success(Some(
              Record(e3.slice, e3.persistenceId, e3.sequenceNr + 1, Instant.ofEpochMilli(e3.timestamp).plusMillis(20))))
            // e1: validation is pending in flatMapConcat
            // e2: waiting on e1 validation
            // e3: validated (duplicate), waiting to emit on e2 emission
            // e4: waiting on e2 validation
            sinkProbe.expectNoMessage(30.millis)
            eventProbe.sendNext(createEnvelope("p3", 1, startTime.minusMillis(921), "e5", clock))
            p1Load.promise.success(None)
            // e1 validation completes, e2 validates immediately, e3 is deduped, e4 validates and emits immediately
            sinkProbe.requestNext()._2 match {
              case FromProjectionContext(event) => event shouldBe "e1"
              case _                            => fail("Unknown context type")
            }
            dao.probesByPid.get("p3").expectMessageType[LoadSequenceNumber].promise.success(None)
            sinkProbe.requestNext()._2 match {
              case FromProjectionContext(event) => event shouldBe "e2"
              case _                            => fail("unknown context type")
            }
            assert(eventProbe.pending > 0, "there should be unfulfilled demand on the source")
            sinkProbe.requestNext()._2 match {
              case FromProjectionContext(event) => event shouldBe "e4"
              case _                            => fail("unknown context type")
            }
            sinkProbe.requestNext()._2 match {
              case FromProjectionContext(event) => event shouldBe "e5"
              case _                            => fail("unknown context type")
            }
          }
          sinkProbe.cancel()
          succeed
      }
    }

    "delay subsequent validation until replay completes" in withHarness { (clock, sourceProvider, dao, offsetStore) =>
      val startTime = clock.instant()

      initializeProbes(Set("p1", "p2"), sourceProvider, dao)
      val source = mappedSource(sourceProvider, offsetStore, FutureNone)
      val sinkProbe = source.runWith(TestSink())
      val (eventProbe, eventSource) = TestSource[EventEnvelope[String]]().preMaterialize()
      sourceProvider.sourceProbe.receiveMessage().promise.success(eventSource)
      eventProbe.ensureSubscription()
      sinkProbe.request(1)

      val p1env1 = createEnvelope("p1", 1, startTime.minusMillis(100), "e1", clock)
      val p1env2 = createEnvelope("p1", 2, startTime.minusMillis(50), "e2", clock)
      eventProbe.sendNext(p1env2) // skip e1
      val p1Load = dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber]
      eventProbe.sendNext(createEnvelope("p1", 3, startTime.minusMillis(25), "e3", clock))
      eventProbe.sendNext(createEnvelope("p2", 1, startTime.minusMillis(75), "e4", clock))
      dao.probesByPid.get("p2").expectMessageType[LoadSequenceNumber].promise.success(None)
      sinkProbe.expectNoMessage(30.millis)

      // e1 is missing, e2 validation is pending load, e3 validation is pending e2 going inflight, e4 is validated (in mapAsyncPartitioned)

      p1Load.promise.success(None) // rejection from query
      val p1TimestampOf = sourceProvider.probesByPid.get("p1").expectMessageType[TimestampOf]
      p1TimestampOf.sequenceNr shouldBe 1
      p1TimestampOf.promise.success(Some(Instant.ofEpochMilli(p1env1.timestamp)))
      // since there's no sequence number persisted, there's another attempt to load the sequence number
      dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber].promise.success(None)
      val p1Replay = sourceProvider.probesByPid.get("p1").expectMessageType[CurrentEventsByPersistenceId]
      p1Replay.from shouldBe 1
      p1Replay.to shouldBe 1
      val (replayProbe, replaySource) = TestSource[EventEnvelope[String]]().preMaterialize()
      p1Replay.promise.success(replaySource)
      replayProbe.ensureSubscription()
      replayProbe.sendNext(p1env1)

      // replay will validate envelopes also
      dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber].promise.success(None)
      replayProbe.sendComplete()

      (1 to 4).iterator
        .map { _ =>
          sinkProbe.requestNext()._2 match {
            case FromProjectionContext(event) => event
            case _                            => fail("unknown context type")
          }
        }
        .mkString("|") shouldBe "e1|e2|e3|e4"
    }

    "drop on rejection without replay" in {
      val clock = Clock.systemUTC()
      val sourceProvider = new ProbeStubSourceProvider(testKit) {
        override private[akka] def currentEventsByPersistenceId(
            persistenceId: String,
            fromSequenceNr: Long,
            toSequenceNr: Long): Option[Source[EventEnvelope[String], NotUsed]] = {
          stubCEP(persistenceId, fromSequenceNr, toSequenceNr)
          None
        }
      }
      val (dao, offsetStore) = createOffsetStore(testKit, clock, Some(sourceProvider))
      val startTime = clock.instant()

      initializeProbes(Set("p1", "p2"), sourceProvider, dao)
      val sinkProbe = mappedSource(sourceProvider, offsetStore, FutureNone).runWith(TestSink())
      val (eventProbe, eventSource) = TestSource[EventEnvelope[String]]().preMaterialize()
      sourceProvider.sourceProbe.receiveMessage().promise.success(eventSource)
      eventProbe.ensureSubscription()
      sinkProbe.request(1)

      eventProbe.sendNext(createEnvelope("p1", 1, startTime.minusMillis(1000), "e1", clock))
      eventProbe.sendNext(createEnvelope("p2", 10, startTime.minusMillis(900), "e12", clock))
      eventProbe.sendNext(createEnvelope("p1", 2, startTime.minusMillis(750), "e2", clock))
      eventProbe.sendNext(createEnvelope("p2", 11, startTime.minusMillis(100), "e13", clock))
      dao.probesByPid.get("p2").expectMessageType[LoadSequenceNumber].promise.success(None) // somehow missed seqNrs 1-9
      dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber].promise.success(None) // accept
      sinkProbe.requestNext()._2 match {
        case FromProjectionContext(event) => event shouldBe "e1"
        case _                            => fail("Unknown context type")
      }
      var p2TimestampOf = sourceProvider.probesByPid.get("p2").expectMessageType[TimestampOf]
      p2TimestampOf.sequenceNr shouldBe 9
      p2TimestampOf.promise.success(Some(startTime.minusSeconds(2))) // trigger rejection of e12
      dao.probesByPid
        .get("p2")
        .expectMessageType[LoadSequenceNumber]
        .promise
        .success(None) // load before replay attempt
      // no need to return a source, just prove that currentEventsByPersistenceId was called
      sourceProvider.probesByPid.get("p2").expectMessageType[CurrentEventsByPersistenceId]
      dao.probesByPid.get("p2").expectMessageType[LoadSequenceNumber].promise.success(None)
      sinkProbe.requestNext()._2 match {
        case FromProjectionContext(event) => event shouldBe "e2"
        case _                            => fail("Unknown context type")
      }
      p2TimestampOf = sourceProvider.probesByPid.get("p2").expectMessageType[TimestampOf]
      p2TimestampOf.sequenceNr shouldBe 10
      p2TimestampOf.promise.success(Some(startTime.minusMillis(900))) // reject e13
      // once again, replay is attempted
      dao.probesByPid.get("p2").expectMessageType[LoadSequenceNumber].promise.success(None)
      sourceProvider.probesByPid.get("p2").expectMessageType[CurrentEventsByPersistenceId]
      sinkProbe.expectNoMessage(30.millis)
    }

    "fail on rejections from backtracking when replay isn't possible" in {
      val clock = Clock.systemUTC()
      val sourceProvider = new ProbeStubSourceProvider(testKit) {
        override private[akka] def currentEventsByPersistenceId(
            persistenceId: String,
            fromSequenceNr: Long,
            toSequenceNr: Long): Option[Source[EventEnvelope[String], NotUsed]] = {
          stubCEP(persistenceId, fromSequenceNr, toSequenceNr)
          None
        }
      }

      val (dao, offsetStore) = createOffsetStore(testKit, clock, Some(sourceProvider))
      val startTime = clock.instant()

      initializeProbes(Set("p1", "p2"), sourceProvider, dao)
      val sinkProbe = mappedSource(sourceProvider, offsetStore, FutureNone).runWith(TestSink())
      val (eventProbe, eventSource) = TestSource[EventEnvelope[String]]().preMaterialize()
      sourceProvider.sourceProbe.receiveMessage().promise.success(eventSource)
      eventProbe.ensureSubscription()
      sinkProbe.request(1)

      // backtracking will load the event payload... there's no need to do this for the other events because of the failure
      val env1 = createEnvelope("p1", 1, startTime.minusMillis(1000), "e1", clock)
      eventProbe.sendNext(backtracking(env1))
      eventProbe.sendNext(backtracking(createEnvelope("p2", 10, startTime.minusMillis(900), "e12", clock)))
      eventProbe.sendNext(backtracking(createEnvelope("p1", 2, startTime.minusMillis(750), "e2", clock)))
      eventProbe.sendNext(backtracking(createEnvelope("p2", 11, startTime.minusMillis(100), "e13", clock)))
      dao.probesByPid.get("p2").expectMessageType[LoadSequenceNumber].promise.success(None)
      dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber].promise.success(None)
      sourceProvider.probesByPid
        .get("p1")
        .expectMessageType[LoadEnvelope[String]]
        .promise
        .success(env1)

      val emittedPC = sinkProbe.requestNext()._2
      emittedPC match {
        case pc: ProjectionContextImpl[_, _] =>
          pc.envelope match {
            case e: EventEnvelope[_] =>
              e.persistenceId shouldBe "p1"
              e.sequenceNr shouldBe 1

            case _ => fail("Not an EventEnvelope")
          }
        case _ => fail("Unknown context type")
      }

      sourceProvider.probesByPid
        .get("p2")
        .expectMessageType[TimestampOf]
        .promise
        .success(Some(startTime.minusSeconds(3)))
      dao.probesByPid.get("p2").expectMessageType[LoadSequenceNumber].promise.success(None)
      sourceProvider.probesByPid.get("p2").expectMessageType[CurrentEventsByPersistenceId]
      val rejected = sinkProbe.expectError()
      rejected shouldBe a[RejectedEnvelope]
      Seq("rejected envelope", "backtracking", "persistenceid [p2], seqnr [10]", "unexpected sequence number").foreach {
        requiredString =>
          rejected.getMessage.toLowerCase should include(requiredString)
      }
    }
  }
}
