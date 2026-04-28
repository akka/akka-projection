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
import akka.persistence.query.typed.EventEnvelope
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Validation

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
    .withAcceptSequenceNumberResetAfter(3600.seconds) // needed for test "...number reset is possible"
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

    "reset state on failed save but not stop" in {
      val clock = Clock.systemUTC()
      val startTime = clock.instant()

      val dao = new ProbeStubOffsetStoreDao(testKit)

      val offsetStore = new DynamoDBOffsetStore(projectionId, uuid(), None, system, baseSettings, dao, clock)

      val t0 = startTime
      val offset0 = TimestampOffset(t0, Map("p1" -> 777L))
      val slice1 = dao.sliceFor("p1")
      val recordP1N1 = Record(slice1, "p1", 776, offset0.timestamp.minusSeconds(400))
      Seq("p1", "p2").foreach { pid =>
        dao.initializeProbes(Some(pid), Some(dao.sliceFor(pid)))
      }

      val firstLoadFuture = offsetStore.load("p1")
      val p1Probe = dao.probesByPid.get("p1")
      p1Probe.expectMessageType[LoadSequenceNumber].promise.success(Some(recordP1N1))
      firstLoadFuture.futureValue.byPid.get("p1").map(_.seqNr) should contain(776L)
      val saveFuture = offsetStore.saveOffset(OffsetPidSeqNr(offset0, "p1", 777))
      // no need to load...
      val storeSequenceNumber = p1Probe.expectMessageType[StoreSequenceNumber]
      storeSequenceNumber.seqNr shouldBe 777
      val secondLoadFuture = offsetStore.load("p2")
      val p2Probe = dao.probesByPid.get("p2")
      p2Probe.expectNoMessage(30.millis)

      storeSequenceNumber.promise.failure(new RuntimeException("DB failure"))
      p2Probe.expectMessageType[LoadSequenceNumber].promise.success(None)

      (the[RuntimeException] thrownBy (saveFuture.futureValue)).getMessage should include("DB failure")
      (secondLoadFuture.futureValue.byPid.keySet shouldNot contain).oneOf("p1", "p2")

      assert(!offsetStore.isStopped(), "offset store should not be stopped")
    }

    "not load when validating in-flight pid unless sequence number reset is possible" in {
      val clock = Clock.systemUTC()
      val startTime = clock.instant()

      val dao = new ProbeStubOffsetStoreDao(testKit)

      val offsetStore = new DynamoDBOffsetStore(projectionId, uuid(), None, system, baseSettings, dao, clock)

      val t0 = startTime.minusSeconds(3610)
      val offset0 = TimestampOffset(t0, Map("p1" -> 1L))
      val env0 = EventEnvelope(offset0, "p1", 1, "e1", t0.toEpochMilli, "", dao.sliceFor("p1"))
      val offset1 = TimestampOffset(t0.plusMillis(100), Map("p1" -> 2))
      val env1 = EventEnvelope(offset1, "p1", 2, "e2", offset1.timestamp.toEpochMilli, "", dao.sliceFor("p1"))
      val t1 = startTime.minusSeconds(5)
      val offset2 = TimestampOffset(t1, Map("p1" -> 1L)) // sequence number reset
      val env2 = EventEnvelope(offset2, "p1", 1, "e1a", offset2.timestamp.toEpochMilli, "", dao.sliceFor("p1"))
      dao.initializeProbes(Some("p1"), None)

      val validation1 = offsetStore.validate(env0)
      dao.probesByPid.get("p1").expectMessageType[LoadSequenceNumber].promise.success(None)
      // offset store state does not have a seqNr for p1
      validation1.futureValue shouldBe Validation.Accepted
      offsetStore.addInflight(env0)
      val validation2 = offsetStore.validate(env1)
      validation2.futureValue shouldBe Validation.Accepted // no load attempted
      dao.probesByPid.get("p1").expectNoMessage(30.millis)
      offsetStore.addInflight(env1)
      // no offset save to remove from inflight and update state
      val validation3 = offsetStore.validate(env2) // reset the sequence number
      // since no save (which would update state and clear inflight) happened, there shouldn't actually be a record
      // for p1 in the offset store, but assume that there now is.
      dao.probesByPid
        .get("p1")
        .expectMessageType[LoadSequenceNumber]
        .promise
        .success(Some(Record(env1.slice, env1.persistenceId, env1.sequenceNr, Instant.ofEpochMilli(env1.timestamp))))
      validation3.futureValue shouldBe Validation.Accepted
    }
  }
}
