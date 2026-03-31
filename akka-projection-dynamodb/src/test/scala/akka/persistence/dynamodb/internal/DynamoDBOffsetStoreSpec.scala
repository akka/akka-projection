/*
 * Copyright (C) 2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.dynamodb.internal

import java.time.Clock
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.persistence.Persistence
import akka.persistence.query.TimestampOffset
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Record
import akka.projection.internal.ManagementState
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.AttemptToUseStoppedOffsetStore

object DynamoDBOffsetStoreSpec {
  val config = {
    val default = ConfigFactory.load()

    ConfigFactory.parseString("""
      akka.loglevel = DEBUG
    """).withFallback(default)
  }

  sealed trait ByPidCommand

  final case class LoadSequenceNumber(promise: Promise[Option[Record]]) extends ByPidCommand
  final case class StoreSequenceNumber(seqNr: Long, timestamp: Instant, promise: Promise[Done]) extends ByPidCommand

  sealed trait BySliceCommand

  final case class StoreTimestampOffset(offset: TimestampOffset, promise: Promise[Done]) extends BySliceCommand

  class ProbeStubOffsetStoreDao(testKit: ActorTestKit) extends OffsetStoreDao {
    import testKit.system

    implicit val ec: ExecutionContext = system.executionContext

    val probesBySlice = new ConcurrentHashMap[Int, TestProbe[BySliceCommand]]()
    val probesByPid = new ConcurrentHashMap[String, TestProbe[ByPidCommand]]()

    def initializeProbes(pid: Option[String], slice: Option[Int]): Unit = {
      pid.foreach { p => probesByPid.putIfAbsent(p, TestProbe()) }
      slice.foreach { s => probesBySlice.putIfAbsent(s, TestProbe()) }
    }

    def transactStoreSequenceNumbers(writeItems: Iterable[TransactWriteItem])(records: Seq[Record]): Future[Done] =
      throw new NotImplementedError("Transactional store not tested in this spec, only in the integration test")

    def readManagementState(slice: Int): Future[Option[ManagementState]] = managementStateNotImplemented
    def updateManagementState(minSlice: Int, maxSlice: Int, paused: Boolean): Future[Done] =
      managementStateNotImplemented

    def loadTimestampOffset(slice: Int): Future[Option[TimestampOffset]] = ???

    def storeTimestampOffsets(offsetsBySlice: Map[Int, TimestampOffset]): Future[Done] = {
      offsetsBySlice.keysIterator.foreach { s =>
        initializeProbes(None, Some(s))
      }

      Future
        .reduceLeft(offsetsBySlice.map {
          case (slice, offset) =>
            val reified = StoreTimestampOffset(offset, Promise())
            probesBySlice.get(slice).ref ! reified
            reified.promise.future
        })((_, _) => Done)
    }

    def storeSequenceNumbers(records: IndexedSeq[Record]): Future[Done] = {
      records.iterator.foreach { r =>
        initializeProbes(Some(r.pid), Some(r.slice))
      }

      Future
        .reduceLeft(records.map { r =>
          val reified = StoreSequenceNumber(r.seqNr, r.timestamp, Promise())
          probesByPid.get(r.pid).ref ! reified
          reified.promise.future
        })((_, _) => Done)
    }

    def loadSequenceNumber(slice: Int, pid: String): Future[Option[Record]] = {
      val reified = LoadSequenceNumber(Promise())
      initializeProbes(Some(pid), Some(slice))
      probesByPid.get(pid).ref ! reified
      reified.promise.future
    }

    def sliceFor(pid: String): Int = persistenceExt.sliceForPersistenceId(pid)

    private val persistenceExt = Persistence(system)

    private def managementStateNotImplemented: Nothing =
      throw new NotImplementedError("Management state not tested in this spec, only in the integration test")
  }

  val projectionId = ProjectionId.of("some-projection", "some-key")

  def uuid() = UUID.randomUUID().toString

  val baseSettings = DynamoDBProjectionSettings(config.getConfig("akka.projection.dynamodb"))
}

class DynamoDBOffsetStoreSpec
    extends ScalaTestWithActorTestKit(DynamoDBOffsetStoreSpec.config)
    with AnyWordSpecLike
    with LogCapturing {
  import DynamoDBOffsetStoreSpec._

  implicit val ec: ExecutionContext = system.executionContext

  "A DynamoDBOffsetStore" should {
    "delay offset load of saved pid until save completes without querying DAO" in {
      val clock = Clock.systemUTC()
      val startTime = clock.instant()

      val dao = new ProbeStubOffsetStoreDao(testKit)

      val offsetStore = new DynamoDBOffsetStore(projectionId, uuid(), None, system, baseSettings, dao, clock)

      val t0 = startTime
      val offset0 = TimestampOffset(t0, Map("p1" -> 42L))
      dao.initializeProbes(Some("p1"), Some(dao.sliceFor("p1")))

      // NB: not adding to inflight before, which is atypical
      val saveFuture = offsetStore.saveOffset(OffsetPidSeqNr(offset0, "p1", 42))
      val loadFromSave = dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber]
      val loadFuture = offsetStore.load("p1")
      dao.probesByPid.get("p1").expectNoMessage(30.millis)
      loadFuture.value should be(empty)
      loadFromSave.promise.success(Some(Record(dao.sliceFor("p1"), "p1", 41, t0.minusMillis(100))))
      val storeSeqNr = dao.probesByPid.get("p1").expectMessageType[StoreSequenceNumber]
      storeSeqNr.seqNr shouldBe 42
      storeSeqNr.timestamp shouldBe t0
      storeSeqNr.promise.success(Done)
      dao.probesByPid.get("p1").expectNoMessage(30.millis)
      val storeTimestampOffset = dao.probesBySlice.get(dao.sliceFor("p1")).expectMessageType[StoreTimestampOffset]
      storeTimestampOffset.offset shouldBe offset0
      storeTimestampOffset.promise.success(Done)

      // the load performed by the save means the load needs not actually go to the dao
      Future
        .sequence(
          Seq(
            // concurrently validate no load activity
            Future { dao.probesBySlice.get(dao.sliceFor("p1")).expectNoMessage(30.millis) },
            Future { dao.probesByPid.get("p1").expectNoMessage(30.millis) }))
        .futureValue

      saveFuture.futureValue
      loadFuture.futureValue
    }

    "delay offset load of different new pid until save completes, then perform I/O" in {
      val clock = Clock.systemUTC()
      val startTime = clock.instant()

      val dao = new ProbeStubOffsetStoreDao(testKit)

      val offsetStore = new DynamoDBOffsetStore(projectionId, uuid(), None, system, baseSettings, dao, clock)

      val t0 = startTime
      val offset0 = TimestampOffset(t0, Map("p1" -> 21L))
      dao.initializeProbes(Some("p1"), Some(dao.sliceFor("p1")))
      dao.initializeProbes(Some("p2"), Some(dao.sliceFor("p2")))

      val saveFuture = offsetStore.saveOffset(OffsetPidSeqNr(offset0, "p1", 21))
      val loadFromSave = dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber]
      val loadFuture = offsetStore.load("p2")
      dao.probesByPid.get("p2").expectNoMessage(30.millis)
      loadFuture.value shouldBe (empty)
      loadFromSave.promise.success(Some(Record(dao.sliceFor("p1"), "p1", 20, t0.minusSeconds(10))))
      val storeSeqNr = dao.probesByPid.get("p1").expectMessageType[StoreSequenceNumber]
      storeSeqNr.seqNr shouldBe 21
      storeSeqNr.promise.success(Done)
      dao.probesByPid.get("p2").expectNoMessage(30.millis)
      dao.probesBySlice.get(dao.sliceFor("p1")).expectMessageType[StoreTimestampOffset].promise.success(Done)

      saveFuture.futureValue shouldBe Done
      dao.probesByPid
        .get("p2")
        .expectMessageType[LoadSequenceNumber]
        .promise
        .success(Some(Record(dao.sliceFor("p2"), "p2", 123, t0.minusSeconds(87654)))) // More than a day ago

      loadFuture.futureValue.byPid("p2").seqNr shouldBe 123
    }

    "CAS retry loads if multiple saves are performed in time to load from DDB" in {
      val clock = Clock.systemUTC()
      val startTime = clock.instant()

      val dao = new ProbeStubOffsetStoreDao(testKit)

      val offsetStore = new DynamoDBOffsetStore(projectionId, uuid(), None, system, baseSettings, dao, clock)

      val t0 = startTime
      val offset0 = TimestampOffset(t0, Map("p1" -> 7L))
      val offset1 = TimestampOffset(t0.plusMillis(22), Map("p1" -> 8L))
      val offset2 = TimestampOffset(t0.plusMillis(35), Map("p1" -> 9L))
      val p2Record = Record(dao.sliceFor("p2"), "p2", 57, t0.minusSeconds(100))
      Seq("p1", "p2").foreach { pid =>
        dao.initializeProbes(Some(pid), Some(dao.sliceFor(pid)))
      }

      val saveFuture1 = offsetStore.saveOffset(OffsetPidSeqNr(offset0, "p1", 7))
      val loadFromSave = dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber]
      val loadFuture = offsetStore.load("p2")
      loadFuture.value shouldBe (empty)
      loadFromSave.promise.success(Some(Record(dao.sliceFor("p1"), "p1", 6, t0.minusSeconds(5))))
      dao.probesByPid.get("p1").expectMessageType[StoreSequenceNumber].promise.success(Done)
      dao.probesBySlice.get(dao.sliceFor("p1")).expectMessageType[StoreTimestampOffset].promise.success(Done)

      saveFuture1.futureValue shouldBe Done
      // "oldState" in load now has saveCounter 1
      var loadFromLoad = dao.probesByPid.get("p2").expectMessageType[LoadSequenceNumber]
      val saveFuture2 = offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 8))
      dao.probesByPid.get("p2").expectNoMessage(30.millis)
      val secondSaveSequenceNr = dao.probesByPid.get("p1").expectMessageType[StoreSequenceNumber]
      secondSaveSequenceNr.seqNr shouldBe 8
      secondSaveSequenceNr.promise.success(Done)
      dao.probesBySlice.get(dao.sliceFor("p1")).expectMessageType[StoreTimestampOffset].promise.success(Done)

      saveFuture2.futureValue shouldBe Done
      val saveFuture3 = offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p1", 9))
      dao.probesByPid.get("p1").expectMessageType[StoreSequenceNumber].promise.success(Done)
      dao.probesBySlice.get(dao.sliceFor("p1")).expectMessageType[StoreTimestampOffset].promise.success(Done)

      saveFuture3.futureValue shouldBe Done
      loadFromLoad.promise.success(Some(p2Record))

      // intervening save requires reload
      loadFromLoad = dao.probesByPid.get("p2").expectMessageType[LoadSequenceNumber]
      loadFromLoad.promise.success(Some(p2Record)) // nothing new was actually written

      loadFuture.futureValue.byPid("p2").seqNr shouldBe 57
    }

    // The current implementation doesn't actually do concurrent single-pid loads in validation, but we can test this
    "merge concurrent loads of 2 pids without retrying I/O" in {
      val clock = Clock.systemUTC()
      val startTime = clock.instant()

      val dao = new ProbeStubOffsetStoreDao(testKit)

      val offsetStore = new DynamoDBOffsetStore(projectionId, uuid(), None, system, baseSettings, dao, clock)

      val t0 = startTime
      val p1Record = Record(dao.sliceFor("p1"), "p1", 82, t0)
      val p2Record = Record(dao.sliceFor("p2"), "p2", 77, t0.plusSeconds(1))
      Seq("p1", "p2").foreach { pid =>
        dao.initializeProbes(Some(pid), None)
      }

      val p1Load = offsetStore.load("p1")
      val p2Load = offsetStore.load("p2")
      dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber].promise.success(Some(p1Record))
      dao.probesByPid.get("p2").expectMessageType[LoadSequenceNumber].promise.success(Some(p2Record))

      Future
        .sequence(Seq("p1", "p2").map { pid => Future { dao.probesByPid.get(pid).expectNoMessage(30.millis) } })
        .futureValue
        .size shouldBe 2

      p1Load
        .zip(p2Load)
        .map {
          case (state1, state2) => state1.byPid.size + state2.byPid.size
        }
        .futureValue shouldBe 3 // one of the states has both pids
    }

    "stop on failed save and propagate" in {
      val clock = Clock.systemUTC()

      val dao = new ProbeStubOffsetStoreDao(testKit)

      val offsetStore = new DynamoDBOffsetStore(projectionId, uuid(), None, system, baseSettings, dao, clock)

      val t0 = clock.instant()
      val offset0 = TimestampOffset(t0, Map("p1" -> 777L))

      val saveFuture = offsetStore.saveOffset(OffsetPidSeqNr(offset0, "p1", 777))
      val loadFromSave = dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber]
      val loadFuture = offsetStore.load("p2")
      loadFuture.value shouldBe (empty)
      loadFromSave.promise.failure(new RuntimeException("DB went boom"))

      saveFuture.failed.futureValue.getMessage should include("DB went boom")
      eventually { assert(offsetStore.isStopped(), "offset store should stop") }
      loadFuture.failed.futureValue.getMessage should include("DB went boom")

      an[AttemptToUseStoppedOffsetStore] shouldBe thrownBy { offsetStore.load("p2") }
    }
  }
}
