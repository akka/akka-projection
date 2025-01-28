/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.DurationConverters._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.query.TimestampOffset
import akka.persistence.query.TimestampOffsetBySlice
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.typed.PersistenceId
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.dynamodb.internal.DynamoDBOffsetStore
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Pid
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.SeqNr
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Validation.Accepted
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Validation.Duplicate
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Validation.RejectedBacktrackingSeqNr
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Validation.RejectedSeqNr
import akka.projection.dynamodb.internal.OffsetPidSeqNr
import akka.projection.dynamodb.internal.OffsetStoreDao
import akka.projection.dynamodb.internal.OffsetStoreDao.OffsetStoreAttributes
import akka.projection.internal.ManagementState
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.OptionValues
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object DynamoDBTimestampOffsetStoreSpec {
  val config: Config = TestConfig.config

  def configWithOffsetTTL: Config =
    ConfigFactory
      .parseString("""
        akka.projection.dynamodb.time-to-live {
          projections {
            "*" {
              offset-time-to-live = 1 hour
            }
          }
        }
      """)
      .withFallback(config)

  class TestTimestampSourceProvider(override val minSlice: Int, override val maxSlice: Int, clock: TestClock)
      extends BySlicesSourceProvider
      with EventTimestampQuery
      with LoadEventQuery {

    override def timestampOf(persistenceId: String, sequenceNr: SeqNr): Future[Option[Instant]] =
      Future.successful(Some(clock.instant()))

    override def loadEnvelope[Event](persistenceId: String, sequenceNr: SeqNr): Future[EventEnvelope[Event]] =
      throw new IllegalStateException("loadEvent shouldn't be used here")
  }
}

class DynamoDBTimestampOffsetStoreSpec
    extends DynamoDBTimestampOffsetStoreBaseSpec(DynamoDBTimestampOffsetStoreSpec.config)

class DynamoDBTimestampOffsetStoreWithOffsetTTLSpec
    extends DynamoDBTimestampOffsetStoreBaseSpec(DynamoDBTimestampOffsetStoreSpec.configWithOffsetTTL) {
  override protected def usingOffsetTTL: Boolean = true
}

abstract class DynamoDBTimestampOffsetStoreBaseSpec(config: Config)
    extends ScalaTestWithActorTestKit(config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with OptionValues
    with LogCapturing {
  import DynamoDBTimestampOffsetStoreSpec.TestTimestampSourceProvider

  override def typedSystem: ActorSystem[_] = system

  private val clock = TestClock.nowMicros()
  def tick(): Unit = clock.tick(JDuration.ofMillis(1))

  private val log = LoggerFactory.getLogger(getClass)

  protected def usingOffsetTTL: Boolean = false

  private def createOffsetStore(
      projectionId: ProjectionId,
      customSettings: DynamoDBProjectionSettings = settings,
      offsetStoreClock: TestClock = clock,
      eventTimestampQueryClock: TestClock = clock) =
    new DynamoDBOffsetStore(
      projectionId,
      Some(new TestTimestampSourceProvider(0, persistenceExt.numberOfSlices - 1, eventTimestampQueryClock)),
      system,
      customSettings,
      client,
      offsetStoreClock)

  def createEnvelope(pid: Pid, seqNr: SeqNr, timestamp: Instant, event: String): EventEnvelope[String] = {
    val entityType = PersistenceId.extractEntityType(pid)
    val slice = persistenceExt.sliceForPersistenceId(pid)
    EventEnvelope(
      TimestampOffset(timestamp, timestamp.plusMillis(1000), Map(pid -> seqNr)),
      pid,
      seqNr,
      event,
      timestamp.toEpochMilli,
      entityType,
      slice)
  }

  def backtrackingEnvelope(env: EventEnvelope[String]): EventEnvelope[String] =
    new EventEnvelope[String](
      env.offset,
      env.persistenceId,
      env.sequenceNr,
      eventOption = None,
      env.timestamp,
      env.internalEventMetadata,
      env.entityType,
      env.slice,
      env.filtered,
      source = EnvelopeOrigin.SourceBacktracking)

  def filteredEnvelope(env: EventEnvelope[String]): EventEnvelope[String] =
    new EventEnvelope[String](
      env.offset,
      env.persistenceId,
      env.sequenceNr,
      env.eventOption,
      env.timestamp,
      env.internalEventMetadata,
      env.entityType,
      env.slice,
      filtered = true,
      env.source)

  def createUpdatedDurableState(
      pid: Pid,
      revision: SeqNr,
      timestamp: Instant,
      state: String): UpdatedDurableState[String] =
    new UpdatedDurableState(
      pid,
      revision,
      state,
      TimestampOffset(timestamp, timestamp.plusMillis(1000), Map(pid -> revision)),
      timestamp.toEpochMilli)

  def slice(pid: String): Int =
    persistenceExt.sliceForPersistenceId(pid)

  "The DynamoDBOffsetStore for TimestampOffset" must {

    "save TimestampOffset with one entry" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffsetBySlice]().futureValue
      readOffset1.get.offsets(slice("p1")) shouldBe offset1
      offsetStore.getState().offsetBySlice(slice("p1")) shouldBe offset1
      offsetStore.storedSeqNr("p1").futureValue shouldBe 3L

      tick()
      val offset2 = TimestampOffset(clock.instant(), Map("p1" -> 4L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p1", 4L)).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffsetBySlice]().futureValue
      readOffset2.get.offsets(slice("p1")) shouldBe offset2 // yep, saveOffset overwrites previous
      offsetStore.getState().offsetBySlice(slice("p1")) shouldBe offset2
      offsetStore.storedSeqNr("p1").futureValue shouldBe 4L

      val timestampOffsetItem = getOffsetItemFor(projectionId, slice("p1")).value
      val seqNrOffsetItem = getOffsetItemFor(projectionId, slice("p1"), "p1").value

      if (usingOffsetTTL) {
        val expected = System.currentTimeMillis / 1000 + 1.hour.toSeconds
        timestampOffsetItem.get(OffsetStoreAttributes.Expiry) shouldBe None // no expiry set on latest-by-slice
        val seqNrOffsetExpiry = seqNrOffsetItem.get(OffsetStoreAttributes.Expiry).value.n.toLong
        seqNrOffsetExpiry should (be <= expected and be > expected - 10) // within 10 seconds
      } else {
        timestampOffsetItem.get(OffsetStoreAttributes.Expiry) shouldBe None
        seqNrOffsetItem.get(OffsetStoreAttributes.Expiry) shouldBe None
      }
    }

    "save TimestampOffset with several seen entries" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      val entityType = nextEntityType()
      val p1 = nextPersistenceId(entityType)
      val s = slice(p1.id)
      val p2 = randomPersistenceIdForSlice(entityType, s)
      val p3 = randomPersistenceIdForSlice(entityType, s)
      val p4 = randomPersistenceIdForSlice(entityType, s)

      tick()
      val offset1a = TimestampOffset(clock.instant(), Map(p1.id -> 3L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1a, p1.id, 3L)).futureValue
      val offset1b = TimestampOffset(clock.instant(), Map(p1.id -> 3L, p2.id -> 1L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1b, p2.id, 1L)).futureValue
      val offset1c = TimestampOffset(clock.instant(), Map(p1.id -> 3L, p2.id -> 1L, p3.id -> 5L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1c, p3.id, 5L)).futureValue

      val readOffset1 = offsetStore.readOffset[TimestampOffsetBySlice]().futureValue
      val expectedOffset1 =
        TimestampOffset(offset1a.timestamp, offset1a.readTimestamp, Map(p1.id -> 3L, p2.id -> 1L, p3.id -> 5L))
      readOffset1.get.offsets(s) shouldBe expectedOffset1

      tick()
      val offset2 = TimestampOffset(clock.instant(), Map(p1.id -> 4L, p3.id -> 6L, p4.id -> 9L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, p3.id, 6L)).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffsetBySlice]().futureValue
      // note that it's not the seen Map in the saveOffset that is used, but the pid, seqNr of saveOffset,
      // so here we have only saved p3 -> 6
      val expectedOffset2 = TimestampOffset(offset2.timestamp, offset2.readTimestamp, Map(p3.id -> 6L))
      readOffset2.get.offsets(s) shouldBe expectedOffset2
    }

    "save TimestampOffset when same timestamp" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      val entityType = nextEntityType()
      val p1 = nextPersistenceId(entityType)
      val s = slice(p1.id)
      val p2 = randomPersistenceIdForSlice(entityType, s)
      val p3 = randomPersistenceIdForSlice(entityType, s)
      val p4 = randomPersistenceIdForSlice(entityType, s)

      tick()
      // the seen map in saveOffset is not used when saving, so using empty Map for simplicity
      val offset1 = TimestampOffset(clock.instant(), Map.empty)
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, p1.id, 3L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, p2.id, 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, p3.id, 5L)).futureValue
      offsetStore.readOffset().futureValue
      val expectedOffset1 = TimestampOffset(clock.instant(), Map(p1.id -> 3L, p2.id -> 1L, p3.id -> 5L))
      offsetStore.getState().offsetBySlice(s) shouldBe expectedOffset1

      // not tick, same timestamp
      val offset2 = TimestampOffset(clock.instant(), Map.empty)
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, p2.id, 2L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, p4.id, 9L)).futureValue
      offsetStore.readOffset().futureValue
      val expectedOffset2 = TimestampOffset(clock.instant(), expectedOffset1.seen ++ Map(p2.id -> 2L, p4.id -> 9L))
      // all should be included since same timestamp
      offsetStore.getState().offsetBySlice(s) shouldBe expectedOffset2

      // saving new with later timestamp
      tick()
      val offset3 = TimestampOffset(clock.instant(), Map(p1.id -> 4L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset3, p1.id, 4L)).futureValue
      offsetStore.readOffset().futureValue
      offsetStore.getState().offsetBySlice(s) shouldBe offset3
    }

    "save batch of TimestampOffsets" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val p1 = "p1"
      val slice1 = persistenceExt.sliceForPersistenceId(p1)
      slice1 shouldBe 449

      val p2 = "p2"
      val slice2 = persistenceExt.sliceForPersistenceId(p2)
      slice2 shouldBe 450

      val p3 = "p10"
      val slice3 = persistenceExt.sliceForPersistenceId(p3)
      slice3 shouldBe 655

      val p4 = "p11"
      val slice4 = persistenceExt.sliceForPersistenceId(p4)
      slice4 shouldBe 656

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map.empty)
      tick()
      val offset2 = TimestampOffset(clock.instant(), Map.empty)
      tick()
      val offset3 = TimestampOffset(clock.instant(), Map.empty)
      val offsetsBatch1 = Vector(
        OffsetPidSeqNr(offset1, p1, 3L),
        OffsetPidSeqNr(offset1, p2, 1L),
        OffsetPidSeqNr(offset1, p3, 5L),
        OffsetPidSeqNr(offset2, p4, 1L),
        OffsetPidSeqNr(offset2, p1, 4L),
        OffsetPidSeqNr(offset3, p2, 2L))

      offsetStore.saveOffsets(offsetsBatch1).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffsetBySlice]().futureValue
      offsetStore.getState().offsetBySlice(slice1) shouldBe TimestampOffset(offset2.timestamp, Map(p1 -> 4L))
      offsetStore.getState().offsetBySlice(slice2) shouldBe TimestampOffset(offset3.timestamp, Map(p2 -> 2L))
      offsetStore.getState().offsetBySlice(slice3) shouldBe TimestampOffset(offset1.timestamp, Map(p3 -> 5L))
      offsetStore.getState().offsetBySlice(slice4) shouldBe TimestampOffset(offset2.timestamp, Map(p4 -> 1L))
      readOffset1.get.offsets shouldBe offsetStore.getState().offsetBySlice

      val state1 = offsetStore.load(Vector(p1, p2, p3, p4)).futureValue
      state1.byPid(p1).seqNr shouldBe 4L
      state1.byPid(p2).seqNr shouldBe 2L
      state1.byPid(p3).seqNr shouldBe 5L
      state1.byPid(p4).seqNr shouldBe 1L

      tick()
      val offset5 = TimestampOffset(clock.instant(), Map(p1 -> 5L))
      offsetStore.saveOffsets(Vector(OffsetPidSeqNr(offset5, p1, 5L))).futureValue

      tick()
      // duplicate
      val offset6 = TimestampOffset(clock.instant(), Map(p2 -> 1L))
      offsetStore.saveOffsets(Vector(OffsetPidSeqNr(offset6, p2, 1L))).futureValue

      tick()
      val offset7 = TimestampOffset(clock.instant(), Map(p1 -> 6L))
      tick()
      val offset8 = TimestampOffset(clock.instant(), Map(p1 -> 7L))
      tick()
      val offset9 = TimestampOffset(clock.instant(), Map(p1 -> 8L))
      val offsetsBatch2 =
        Vector(OffsetPidSeqNr(offset7, p1, 6L), OffsetPidSeqNr(offset8, p1, 7L), OffsetPidSeqNr(offset9, p1, 8L))

      offsetStore.saveOffsets(offsetsBatch2).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffsetBySlice]().futureValue
      val state2 = offsetStore.load(Vector(p1, p2, p3, p4)).futureValue
      state2.byPid(p1).seqNr shouldBe 8L
      state2.byPid(p2).seqNr shouldBe 2L // duplicate with lower seqNr not saved
      state2.byPid(p3).seqNr shouldBe 5L
      state2.byPid(p4).seqNr shouldBe 1L
      readOffset2.get.offsets shouldBe offsetStore.getState().offsetBySlice

      for (pid <- Seq(p1, p2, p3, p4)) {
        val timestampOffsetItem = getOffsetItemFor(projectionId, slice(pid)).value
        val seqNrOffsetItem = getOffsetItemFor(projectionId, slice(pid), pid).value

        if (usingOffsetTTL) {
          val expected = System.currentTimeMillis / 1000 + 1.hour.toSeconds
          timestampOffsetItem.get(OffsetStoreAttributes.Expiry) shouldBe None // no expiry set on latest-by-slice
          val seqNrOffsetExpiry = seqNrOffsetItem.get(OffsetStoreAttributes.Expiry).value.n.toLong
          seqNrOffsetExpiry should (be <= expected and be > expected - 10) // within 10 seconds
        } else {
          timestampOffsetItem.get(OffsetStoreAttributes.Expiry) shouldBe None
          seqNrOffsetItem.get(OffsetStoreAttributes.Expiry) shouldBe None
        }
      }
    }

    "save batch of many TimestampOffsets" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      def test(pidPrefix: String, numberOfOffsets: Int): Unit = {
        withClue(s"with $numberOfOffsets offsets: ") {
          val offsetsBatch = (1 to numberOfOffsets).map { n =>
            tick()
            val offset = TimestampOffset(clock.instant(), Map.empty)
            OffsetPidSeqNr(offset, s"$pidPrefix$n", n)
          }
          offsetStore.saveOffsets(offsetsBatch).futureValue
          offsetStore.readOffset().futureValue
          (1 to numberOfOffsets).map { n =>
            val pid = s"$pidPrefix$n"
            val state = offsetStore.load(pid).futureValue
            state.byPid(pid).seqNr shouldBe n
          }
        }
      }

      test("a", settings.offsetBatchSize)
      test("a", settings.offsetBatchSize - 1)
      test("a", settings.offsetBatchSize + 1)
      test("a", settings.offsetBatchSize * 2)
      test("a", settings.offsetBatchSize * 2 - 1)
      test("a", settings.offsetBatchSize * 2 + 1)
    }

    "perf save batch of TimestampOffsets" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val warmupIterations = 1 // increase this for serious testing
      val iterations = 2000 // increase this for serious testing
      val batchSize = 100

      // warmup
      (1 to warmupIterations).foreach { _ =>
        val offsets = (1 to batchSize).map { n =>
          val offset = TimestampOffset(Instant.now(), Map(s"p$n" -> 1L))
          OffsetPidSeqNr(offset, s"p$n", 1L)
        }
        Await.result(offsetStore.saveOffsets(offsets), 5.seconds)
      }

      val totalStartTime = System.nanoTime()
      var startTime = System.nanoTime()
      var count = 0

      (1 to iterations).foreach { i =>
        val offsets = (1 to batchSize).map { n =>
          val offset = TimestampOffset(Instant.now(), Map(s"p$n" -> 1L))
          OffsetPidSeqNr(offset, s"p$n", 1L)
        }
        count += batchSize
        Await.result(offsetStore.saveOffsets(offsets), 5.seconds)

        if (i % 1000 == 0) {
          val totalDurationMs = (System.nanoTime() - totalStartTime) / 1000 / 1000
          val durationMs = (System.nanoTime() - startTime) / 1000 / 1000
          log.debug(
            s"#${i * batchSize}: $count took $durationMs ms, RPS ${1000L * count / durationMs}, Total RPS ${1000L * i * batchSize / totalDurationMs}")
          startTime = System.nanoTime()
          count = 0
        }
      }
    }

    "not update when earlier seqNr" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffsetBySlice]().futureValue
      readOffset1.get.offsets(slice("p1")) shouldBe offset1

      clock.setInstant(clock.instant().minusMillis(1))
      val offset2 = TimestampOffset(clock.instant(), Map("p1" -> 2L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p1", 2L)).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffsetBySlice]().futureValue
      readOffset2.get.offsets(slice("p1")) shouldBe offset1 // keeping offset1
    }

    "readOffset from given slices" in {
      val projectionId0 = ProjectionId(UUID.randomUUID().toString, "0-1023")
      val projectionId1 = ProjectionId(projectionId0.name, "0-511")
      val projectionId2 = ProjectionId(projectionId0.name, "512-1023")

      val p1 = "p1"
      val slice1 = persistenceExt.sliceForPersistenceId(p1)
      slice1 shouldBe 449

      val p2 = "p2"
      val slice2 = persistenceExt.sliceForPersistenceId(p2)
      slice2 shouldBe 450

      val p3 = "p10"
      val slice3 = persistenceExt.sliceForPersistenceId(p3)
      slice3 shouldBe 655

      val p4 = "p11"
      val slice4 = persistenceExt.sliceForPersistenceId(p4)
      slice4 shouldBe 656

      val offsetStore0 =
        new DynamoDBOffsetStore(
          projectionId0,
          Some(new TestTimestampSourceProvider(0, persistenceExt.numberOfSlices - 1, clock)),
          system,
          settings,
          client)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map(p1 -> 3L))
      offsetStore0.saveOffset(OffsetPidSeqNr(offset1, p1, 3L)).futureValue
      tick()
      val offset2 = TimestampOffset(clock.instant(), Map(p2 -> 4L))
      offsetStore0.saveOffset(OffsetPidSeqNr(offset2, p2, 4L)).futureValue
      tick()
      val offset3 = TimestampOffset(clock.instant(), Map(p3 -> 7L))
      offsetStore0.saveOffset(OffsetPidSeqNr(offset3, p3, 7L)).futureValue
      tick()
      val offset4 = TimestampOffset(clock.instant(), Map(p4 -> 5L))
      offsetStore0.saveOffset(OffsetPidSeqNr(offset4, p4, 5L)).futureValue

      val offsetStore1 =
        new DynamoDBOffsetStore(
          projectionId1,
          Some(new TestTimestampSourceProvider(0, 511, clock)),
          system,
          settings,
          client)
      offsetStore1.readOffset().futureValue
      // FIXME this is not really testing anything, the test is supposed to test that it is responsible for a range
      val state1 = offsetStore1.load(Vector(p1, p2)).futureValue
      state1.byPid.keySet shouldBe Set(p1, p2)

      val offsetStore2 =
        new DynamoDBOffsetStore(
          projectionId2,
          Some(new TestTimestampSourceProvider(512, 1023, clock)),
          system,
          settings,
          client)
      offsetStore2.readOffset().futureValue
      // FIXME this is not really testing anything, the test is supposed to test that it is responsible for a range
      val state2 = offsetStore2.load(Vector(p3, p4)).futureValue
      state2.byPid.keySet shouldBe Set(p3, p4)
    }

    "filter duplicates" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L, "p2" -> 1L, "p3" -> 5L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p2", 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p3", 5L)).futureValue
      tick()
      val offset2 = TimestampOffset(clock.instant(), Map("p1" -> 4L, "p3" -> 6L, "p4" -> 9L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p1", 4L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p3", 6L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p4", 9L)).futureValue
      tick()
      val offset3 = TimestampOffset(clock.instant(), Map("p5" -> 10L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset3, "p5", 10L)).futureValue

      def env(pid: Pid, seqNr: SeqNr, timestamp: Instant): EventEnvelope[String] =
        createEnvelope(pid, seqNr, timestamp, "evt")

      offsetStore.validate(env("p5", 10, offset3.timestamp)).futureValue shouldBe Duplicate
      offsetStore.validate(env("p1", 4, offset2.timestamp)).futureValue shouldBe Duplicate
      offsetStore.validate(env("p3", 6, offset2.timestamp)).futureValue shouldBe Duplicate
      offsetStore.validate(env("p4", 9, offset2.timestamp)).futureValue shouldBe Duplicate

      offsetStore.validate(env("p1", 3, offset1.timestamp)).futureValue shouldBe Duplicate
      offsetStore.validate(env("p2", 1, offset1.timestamp)).futureValue shouldBe Duplicate
      offsetStore.validate(env("p3", 5, offset1.timestamp)).futureValue shouldBe Duplicate

      offsetStore.validate(env("p1", 2, offset1.timestamp.minusMillis(1))).futureValue shouldBe Duplicate
      offsetStore.validate(env("p5", 9, offset3.timestamp.minusMillis(1))).futureValue shouldBe Duplicate

      offsetStore.validate(env("p5", 11, offset3.timestamp)).futureValue shouldNot be(Duplicate)
      offsetStore.validate(env("p5", 12, offset3.timestamp.plusMillis(1))).futureValue shouldNot be(Duplicate)

      offsetStore.validate(env("p6", 1, offset3.timestamp.plusMillis(2))).futureValue shouldNot be(Duplicate)
      offsetStore.validate(env("p7", 1, offset3.timestamp.minusMillis(1))).futureValue shouldNot be(Duplicate)
    }

    "accept known sequence numbers and reject unknown" in {
      val projectionId = genRandomProjectionId()
      val offsetStoreClock = TestClock.nowMicros()
      val eventTimestampQueryClock = TestClock.nowMicros()
      val offsetStore = createOffsetStore(
        projectionId,
        offsetStoreClock = offsetStoreClock,
        eventTimestampQueryClock = eventTimestampQueryClock)

      val startTime = offsetStoreClock.instant()
      val offset1 = TimestampOffset(startTime, Map("p1" -> 3L, "p2" -> 1L, "p3" -> 5L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p2", 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p3", 5L)).futureValue

      // seqNr 1 is always accepted
      val env1 = createEnvelope("p4", 1L, startTime.plusMillis(1), "e4-1")
      offsetStore.validate(env1).futureValue shouldBe Accepted
      offsetStore.validate(backtrackingEnvelope(env1)).futureValue shouldBe Accepted
      // but not if already inflight, seqNr 1 was accepted
      offsetStore.addInflight(env1)
      val env1Later = createEnvelope("p4", 1L, startTime.plusMillis(1), "e4-1")
      offsetStore.validate(env1Later).futureValue shouldBe Duplicate
      offsetStore.validate(backtrackingEnvelope(env1Later)).futureValue shouldBe Duplicate
      // subsequent seqNr is accepted
      val env2 = createEnvelope("p4", 2L, startTime.plusMillis(2), "e4-2")
      offsetStore.validate(env2).futureValue shouldBe Accepted
      offsetStore.validate(backtrackingEnvelope(env2)).futureValue shouldBe Accepted
      offsetStore.addInflight(env2)
      // but not when gap
      val envP4SeqNr4 = createEnvelope("p4", 4L, startTime.plusMillis(3), "e4-4")
      offsetStore.validate(envP4SeqNr4).futureValue shouldBe RejectedSeqNr
      // hard reject when gap from backtracking
      offsetStore.validate(backtrackingEnvelope(envP4SeqNr4)).futureValue shouldBe RejectedBacktrackingSeqNr
      // reject filtered event when gap
      offsetStore.validate(filteredEnvelope(envP4SeqNr4)).futureValue shouldBe RejectedSeqNr
      // hard reject when filtered event with gap from backtracking
      offsetStore
        .validate(backtrackingEnvelope(filteredEnvelope(envP4SeqNr4)))
        .futureValue shouldBe RejectedBacktrackingSeqNr
      // and not if later already inflight, seqNr 2 was accepted
      offsetStore.validate(createEnvelope("p4", 1L, startTime.plusMillis(1), "e4-1")).futureValue shouldBe Duplicate

      // +1 to known is accepted
      val env3 = createEnvelope("p1", 4L, startTime.plusMillis(4), "e1-4")
      offsetStore.validate(env3).futureValue shouldBe Accepted
      // but not same
      offsetStore.validate(createEnvelope("p3", 5L, startTime, "e3-5")).futureValue shouldBe Duplicate
      // but not same, even if it's 1
      offsetStore.validate(createEnvelope("p2", 1L, startTime, "e2-1")).futureValue shouldBe Duplicate
      // and not less
      offsetStore.validate(createEnvelope("p3", 4L, startTime, "e3-4")).futureValue shouldBe Duplicate
      offsetStore.addInflight(env3)
      // and then it's not accepted again
      offsetStore.validate(env3).futureValue shouldBe Duplicate
      offsetStore.validate(backtrackingEnvelope(env3)).futureValue shouldBe Duplicate
      // and not when later seqNr is inflight
      offsetStore.validate(env2).futureValue shouldBe Duplicate
      offsetStore.validate(backtrackingEnvelope(env2)).futureValue shouldBe Duplicate

      // +1 to known, and then also subsequent are accepted (needed for grouped)
      val env4 = createEnvelope("p3", 6L, startTime.plusMillis(5), "e3-6")
      offsetStore.validate(env4).futureValue shouldBe Accepted
      offsetStore.addInflight(env4)
      val env5 = createEnvelope("p3", 7L, startTime.plusMillis(6), "e3-7")
      offsetStore.validate(env5).futureValue shouldBe Accepted
      offsetStore.addInflight(env5)
      val env6 = createEnvelope("p3", 8L, startTime.plusMillis(7), "e3-8")
      offsetStore.validate(env6).futureValue shouldBe Accepted
      offsetStore.addInflight(env6)

      // reject unknown
      val env7 = createEnvelope("p5", 7L, startTime.plusMillis(8), "e5-7")
      offsetStore.validate(env7).futureValue shouldBe RejectedSeqNr
      offsetStore.validate(backtrackingEnvelope(env7)).futureValue shouldBe RejectedBacktrackingSeqNr
      if (usingOffsetTTL) {
        // but ok when previous is older than expiry window
        val now = offsetStoreClock.tick(JDuration.ofSeconds(10))
        val offsetExpiry = settings.timeToLiveSettings.projections.get(projectionId.name).offsetTimeToLive.value
        eventTimestampQueryClock.withInstant(now.minusSeconds(offsetExpiry.toSeconds + 1)) {
          val env8 = createEnvelope("p5", 7L, startTime.plusMillis(5), "e5-7")
          offsetStore.validate(env8).futureValue shouldBe Accepted
          offsetStore.addInflight(env8)
        }
        // and subsequent seqNr is accepted
        val env9 = createEnvelope("p5", 8L, startTime.plusMillis(9), "e5-8")
        offsetStore.validate(env9).futureValue shouldBe Accepted
        offsetStore.addInflight(env9)
      }

      // reject unknown filtered
      val env10 = filteredEnvelope(createEnvelope("p6", 7L, startTime.plusMillis(10), "e6-7"))
      offsetStore.validate(env10).futureValue shouldBe RejectedSeqNr
      // hard reject when unknown from backtracking
      offsetStore.validate(backtrackingEnvelope(env10)).futureValue shouldBe RejectedBacktrackingSeqNr
      // hard reject when unknown filtered event from backtracking
      offsetStore
        .validate(backtrackingEnvelope(filteredEnvelope(env10)))
        .futureValue shouldBe RejectedBacktrackingSeqNr

      // it's keeping the inflight that are not in the "stored" state
      if (usingOffsetTTL) {
        offsetStore.getInflight() shouldBe Map("p1" -> 4L, "p3" -> 8L, "p4" -> 2L, "p5" -> 8L)
      } else {
        offsetStore.getInflight() shouldBe Map("p1" -> 4L, "p3" -> 8L, "p4" -> 2L)
      }
      // and they are removed from inflight once they have been stored
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(2), Map("p4" -> 2L)), "p4", 2L))
        .futureValue
      if (usingOffsetTTL) {
        offsetStore
          .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(9), Map("p5" -> 8L)), "p5", 8L))
          .futureValue
      }
      offsetStore.getInflight() shouldBe Map("p1" -> 4L, "p3" -> 8L)
    }

    "update inflight on error and re-accept element" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val startTime = TestClock.nowMicros().instant()

      val envelope1 = createEnvelope("p1", 1L, startTime.plusMillis(1), "e1-1")
      val envelope2 = createEnvelope("p1", 2L, startTime.plusMillis(2), "e1-2")
      val envelope3 = createEnvelope("p1", 3L, startTime.plusMillis(2), "e1-2")

      // seqNr 1 is always accepted
      offsetStore.validate(envelope1).futureValue shouldBe Accepted
      offsetStore.addInflight(envelope1)
      offsetStore.getInflight() shouldBe Map("p1" -> 1L)
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(1), Map("p1" -> 1L)), "p1", 1L))
        .futureValue
      offsetStore.getInflight() shouldBe empty

      // seqNr 2 is accepts since it follows seqNr 1 that is stored in state
      offsetStore.validate(envelope2).futureValue shouldBe Accepted
      // simulate envelope processing error by not adding envelope2 to inflight

      // seqNr 3 is not accepted, still waiting for seqNr 2
      offsetStore.validate(envelope3).futureValue shouldBe RejectedSeqNr

      // offer seqNr 2 once again
      offsetStore.validate(envelope2).futureValue shouldBe Accepted
      offsetStore.addInflight(envelope2)
      offsetStore.getInflight() shouldBe Map("p1" -> 2L)

      // offer seqNr 3  once more
      offsetStore.validate(envelope3).futureValue shouldBe Accepted
      offsetStore.addInflight(envelope3)
      offsetStore.getInflight() shouldBe Map("p1" -> 3L)

      // and they are removed from inflight once they have been stored
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(2), Map("p1" -> 3L)), "p1", 3L))
        .futureValue
      offsetStore.getInflight() shouldBe empty
    }

    "mapIsAccepted" in {
      val projectionId = genRandomProjectionId()
      val startTime = TestClock.nowMicros().instant()
      val offsetStore = createOffsetStore(projectionId)

      val offset1 = TimestampOffset(startTime, Map("p1" -> 3L, "p2" -> 1L, "p3" -> 5L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p2", 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p3", 5L)).futureValue

      // seqNr 1 is always accepted
      val env1 = createEnvelope("p4", 1L, startTime.plusMillis(1), "e4-1")
      // subsequent seqNr is accepted
      val env2 = createEnvelope("p4", 2L, startTime.plusMillis(2), "e4-2")
      // but not when gap
      val env3 = createEnvelope("p4", 4L, startTime.plusMillis(3), "e4-4")
      // ok when previous is known
      val env4 = createEnvelope("p1", 4L, startTime.plusMillis(5), "e1-4")
      // but not when previous is unknown
      val env5 = createEnvelope("p3", 7L, startTime.plusMillis(5), "e3-7")

      offsetStore.validateAll(List(env1, env2, env3, env4, env5)).futureValue shouldBe List(
        env1 -> Accepted,
        env2 -> Accepted,
        env3 -> RejectedSeqNr,
        env4 -> Accepted,
        env5 -> RejectedSeqNr)

    }

    "accept new revisions for durable state" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val startTime = TestClock.nowMicros().instant()
      val offset1 = TimestampOffset(startTime, Map("p1" -> 3L, "p2" -> 1L, "p3" -> 5L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p2", 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p3", 5L)).futureValue

      // seqNr 1 is always accepted
      val env1 = createUpdatedDurableState("p4", 1L, startTime.plusMillis(1), "s4-1")
      offsetStore.validate(env1).futureValue shouldBe Accepted
      // but not if already inflight, seqNr 1 was accepted
      offsetStore.addInflight(env1)
      offsetStore
        .validate(createUpdatedDurableState("p4", 1L, startTime.plusMillis(1), "s4-1"))
        .futureValue shouldBe Duplicate
      // subsequent seqNr is accepted
      val env2 = createUpdatedDurableState("p4", 2L, startTime.plusMillis(2), "s4-2")
      offsetStore.validate(env2).futureValue shouldBe Accepted
      offsetStore.addInflight(env2)
      // and also ok with gap
      offsetStore
        .validate(createUpdatedDurableState("p4", 4L, startTime.plusMillis(3), "s4-4"))
        .futureValue shouldBe Accepted
      // and not if later already inflight, seqNr 2 was accepted
      offsetStore
        .validate(createUpdatedDurableState("p4", 1L, startTime.plusMillis(1), "s4-1"))
        .futureValue shouldBe Duplicate

      // greater than known is accepted
      val env3 = createUpdatedDurableState("p1", 4L, startTime.plusMillis(4), "s1-4")
      offsetStore.validate(env3).futureValue shouldBe Accepted
      // but not same
      offsetStore.validate(createUpdatedDurableState("p3", 5L, startTime, "s3-5")).futureValue shouldBe Duplicate
      // but not same, even if it's 1
      offsetStore.validate(createUpdatedDurableState("p2", 1L, startTime, "s2-1")).futureValue shouldBe Duplicate
      // and not less
      offsetStore.validate(createUpdatedDurableState("p3", 4L, startTime, "s3-4")).futureValue shouldBe Duplicate
      offsetStore.addInflight(env3)

      // greater than known, and then also subsequent are accepted (needed for grouped)
      val env4 = createUpdatedDurableState("p3", 8L, startTime.plusMillis(5), "s3-6")
      offsetStore.validate(env4).futureValue shouldBe Accepted
      offsetStore.addInflight(env4)
      val env5 = createUpdatedDurableState("p3", 9L, startTime.plusMillis(6), "s3-7")
      offsetStore.validate(env5).futureValue shouldBe Accepted
      offsetStore.addInflight(env5)
      val env6 = createUpdatedDurableState("p3", 20L, startTime.plusMillis(7), "s3-8")
      offsetStore.validate(env6).futureValue shouldBe Accepted
      offsetStore.addInflight(env6)

      // accept unknown
      val env7 = createUpdatedDurableState("p5", 7L, startTime.plusMillis(8), "s5-7")
      offsetStore.validate(env7).futureValue shouldBe Accepted
      offsetStore.addInflight(env7)

      // it's keeping the inflight that are not in the "stored" state
      offsetStore.getInflight() shouldBe Map("p1" -> 4L, "p3" -> 20L, "p4" -> 2L, "p5" -> 7L)
      // and they are removed from inflight once they have been stored
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(2), Map("p4" -> 2L)), "p4", 2L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(9), Map("p5" -> 8L)), "p5", 8L))
        .futureValue
      offsetStore.getInflight() shouldBe Map("p1" -> 4L, "p3" -> 20L)
    }

    "cleanup inFlight when saving offset" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val startTime = TestClock.nowMicros().instant()
      val offset1 = TimestampOffset(startTime, Map("p1" -> 3L))
      val envelope1 = createEnvelope("p1", 3L, offset1.timestamp, "e1-3")
      val offset2 = TimestampOffset(startTime.plusMillis(1), Map("p1" -> 4L))
      val envelope2 = createEnvelope("p1", 4L, offset2.timestamp, "e1-4")
      val offset3 = TimestampOffset(startTime.plusMillis(2), Map("p1" -> 5L))

      // save same seqNr as inFlight should remove from inFlight
      offsetStore.addInflight(envelope1)
      offsetStore.getInflight().get("p1") shouldBe Some(3L)
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.getInflight().get("p1") shouldBe None

      // clear
      offsetStore.readOffset().futureValue

      // save lower seqNr than inFlight should not remove from inFlight
      offsetStore.addInflight(envelope1)
      offsetStore.getInflight().get("p1") shouldBe Some(3L)
      offsetStore.addInflight(envelope2)
      offsetStore.getInflight().get("p1") shouldBe Some(4L)
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.getInflight().get("p1") shouldBe Some(4L)

      // clear
      offsetStore.readOffset().futureValue

      // save higher seqNr than inFlight should remove from inFlight
      offsetStore.addInflight(envelope1)
      offsetStore.getInflight().get("p1") shouldBe Some(3L)
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p1", 4L)).futureValue
      offsetStore.getInflight().get("p1") shouldBe None

      // clear
      offsetStore.readOffset().futureValue

      // save higher seqNr than inFlight should remove from inFlight
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.addInflight(envelope2)
      offsetStore.getInflight().get("p1") shouldBe Some(4L)
      offsetStore.saveOffset(OffsetPidSeqNr(offset3, "p1", 5L)).futureValue
      offsetStore.getInflight().get("p1") shouldBe None
    }

    "evict old records from same slice" in {
      val projectionId = genRandomProjectionId()
      val evictSettings = settings.withTimeWindow(JDuration.ofSeconds(100))
      import evictSettings._
      val offsetStore = createOffsetStore(projectionId, evictSettings)

      val startTime = TestClock.nowMicros().instant()
      log.debug("Start time [{}]", startTime)

      // these pids have the same slice 645
      val p1 = "p500"
      val p2 = "p621"
      val p3 = "p742"
      val p4 = "p863"
      val p5 = "p984"
      val p6 = "p3080"
      val p7 = "p4290"
      val p8 = "p20180"

      offsetStore.saveOffset(OffsetPidSeqNr(TimestampOffset(startTime, Map(p1 -> 1L)), p1, 1L)).futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(1), Map(p2 -> 1L)), p2, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(2), Map(p3 -> 1L)), p3, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(3), Map(p4 -> 1L)), p4, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(4), Map(p4 -> 1L)), p4, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(5), Map(p5 -> 1L)), p5, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(4), Map(p6 -> 1L)), p6, 3L))
        .futureValue
      offsetStore.getState().size shouldBe 6

      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(timeWindow.minusSeconds(10)), Map(p7 -> 1L)), p7, 1L))
        .futureValue
      offsetStore.getState().size shouldBe 7 // nothing evicted yet

      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(timeWindow.minusSeconds(1)), Map(p8 -> 1L)), p8, 1L))
        .futureValue
      offsetStore.getState().size shouldBe 8 // still nothing evicted yet

      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(timeWindow.plusSeconds(4)), Map(p8 -> 2L)), p8, 2L))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p5, p6, p7, p8)

      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(timeWindow.plusSeconds(20)), Map(p8 -> 3L)), p8, 3L))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p7, p8)
    }

    "evict old records from different slices" in {
      val projectionId = genRandomProjectionId()
      val evictSettings = settings.withTimeWindow(JDuration.ofSeconds(100))
      import evictSettings._
      val offsetStore = createOffsetStore(projectionId, evictSettings)

      val startTime = TestClock.nowMicros().instant()
      log.debug("Start time [{}]", startTime)

      // these pids have the same slice 645
      val p1 = "p500"
      val p2 = "p621"
      val p3 = "p742"
      val p4 = "p863"
      val p5 = "p984"
      val p6 = "p3080"
      val p7 = "p4290"
      val p8 = "p20180"
      val p9 = "p-0960" // slice 576

      offsetStore.saveOffset(OffsetPidSeqNr(TimestampOffset(startTime, Map(p1 -> 1L)), p1, 1L)).futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(1), Map(p2 -> 1L)), p2, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(2), Map(p3 -> 1L)), p3, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(3), Map(p4 -> 1L)), p4, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(4), Map(p4 -> 1L)), p4, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(5), Map(p5 -> 1L)), p5, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusSeconds(4), Map(p6 -> 1L)), p6, 3L))
        .futureValue
      offsetStore.getState().size shouldBe 6

      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(timeWindow.minusSeconds(10)), Map(p7 -> 1L)), p7, 1L))
        .futureValue
      offsetStore.getState().size shouldBe 7 // nothing evicted yet

      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(timeWindow.minusSeconds(1)), Map(p8 -> 1L)), p8, 1L))
        .futureValue
      offsetStore.getState().size shouldBe 8 // still nothing evicted yet

      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(timeWindow.plusSeconds(4)), Map(p8 -> 2L)), p8, 2L))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p5, p6, p7, p8)

      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(timeWindow.plusSeconds(20)), Map(p8 -> 3L)), p8, 3L))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p7, p8)

      // save same slice, but behind
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(1001), Map(p2 -> 2L)), p2, 2L))
        .futureValue
      // it's evicted immediately
      offsetStore.getState().byPid.keySet shouldBe Set(p7, p8)
      val dao = new OffsetStoreDao(system, settings, projectionId, client)
      // but still saved
      dao.loadSequenceNumber(slice(p2), p2).futureValue.get.seqNr shouldBe 2
      // the timestamp was earlier than previously used for this slice, and therefore stored timestamp not changed
      dao.loadTimestampOffset(slice(p2)).futureValue.get.timestamp shouldBe startTime.plus(timeWindow.plusSeconds(20))

      // save another slice that hasn't been used before
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(1002), Map(p9 -> 1L)), p9, 1L))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p9, p7, p8)
      dao.loadSequenceNumber(slice(p9), p9).futureValue.get.seqNr shouldBe 1
      dao.loadTimestampOffset(slice(p9)).futureValue.get.timestamp shouldBe startTime.plusMillis(1002)
      // and one more of that same slice
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(1003), Map(p9 -> 2L)), p9, 2L))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p9, p7, p8)
      dao.loadSequenceNumber(slice(p9), p9).futureValue.get.seqNr shouldBe 2
      dao.loadTimestampOffset(slice(p9)).futureValue.get.timestamp shouldBe startTime.plusMillis(1003)
    }

    "start from slice offset" in {
      val projectionId1 = ProjectionId(UUID.randomUUID().toString, "512-767")
      val projectionId2 = ProjectionId(projectionId1.name, "768-1023")
      val projectionId3 = ProjectionId(projectionId1.name, "512-1023")
      val offsetStore1 = new DynamoDBOffsetStore(
        projectionId1,
        Some(new TestTimestampSourceProvider(512, 767, clock)),
        system,
        settings,
        client)
      val offsetStore2 = new DynamoDBOffsetStore(
        projectionId2,
        Some(new TestTimestampSourceProvider(768, 1023, clock)),
        system,
        settings,
        client)

      val p1 = "p500" // slice 645, projection1
      val p2 = "p863" // slice 645, projection1
      val p3 = "p11" // slice 656, projection1
      val p4 = "p92" // slice 905, projection2

      val time1 = TestClock.nowMicros().instant()
      val time2 = time1.plusSeconds(1)
      val time3 = time1.plusSeconds(2)
      val time4 = time1.plusSeconds(3 * 60) // far ahead

      offsetStore1.saveOffset(OffsetPidSeqNr(TimestampOffset(time1, Map(p1 -> 1L)), p1, 1L)).futureValue
      offsetStore1.saveOffset(OffsetPidSeqNr(TimestampOffset(time2, Map(p2 -> 1L)), p2, 1L)).futureValue
      offsetStore1.saveOffset(OffsetPidSeqNr(TimestampOffset(time3, Map(p3 -> 1L)), p3, 1L)).futureValue
      offsetStore2
        .saveOffset(OffsetPidSeqNr(TimestampOffset(time4, Map(p4 -> 1L)), p4, 1L))
        .futureValue

      // after downscaling
      val offsetStore3 = new DynamoDBOffsetStore(
        projectionId3,
        Some(new TestTimestampSourceProvider(512, 1023, clock)),
        system,
        settings,
        client)

      val offsetBySlice = offsetStore3
        .readOffset[TimestampOffsetBySlice]() // this will load from database
        .futureValue
        .get
        .offsets

      offsetBySlice.size shouldBe 3
      offsetBySlice(slice(p1)).timestamp shouldBe time2
      offsetBySlice(slice(p2)).timestamp shouldBe time2
      offsetBySlice(slice(p3)).timestamp shouldBe time3
      offsetBySlice(slice(p4)).timestamp shouldBe time4

      // getOffset is used by management api, and that should not be adjusted
      TimestampOffset.toTimestampOffset(offsetStore3.getOffset().futureValue.get).timestamp shouldBe time4
    }

    "read and save paused" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      offsetStore.readManagementState().futureValue shouldBe None

      offsetStore.savePaused(paused = true).futureValue
      offsetStore.readManagementState().futureValue shouldBe Some(ManagementState(paused = true))

      offsetStore.savePaused(paused = false).futureValue
      offsetStore.readManagementState().futureValue shouldBe Some(ManagementState(paused = false))

      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.savePaused(paused = true).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffsetBySlice]().futureValue
      readOffset1.get.offsets(slice("p1")) shouldBe offset1
      offsetStore.readManagementState().futureValue shouldBe Some(ManagementState(paused = true))
    }

    // FIXME more tests, see r2dbc
    //    "set offset" in {
    //    "clear offset" in {

    "validate timestamp of previous sequence number" in {
      import DynamoDBOffsetStore.Validation._

      val projectionName = UUID.randomUUID().toString

      val offsetStoreClock = TestClock.nowMicros()
      val eventTimestampQueryClock = TestClock.nowMicros()

      def offsetStore(minSlice: Int, maxSlice: Int) =
        new DynamoDBOffsetStore(
          ProjectionId(projectionName, s"$minSlice-$maxSlice"),
          Some(new TestTimestampSourceProvider(minSlice, maxSlice, eventTimestampQueryClock)),
          system,
          settings,
          client,
          offsetStoreClock)

      // one projection at lower scale
      val offsetStore1 = offsetStore(512, 1023)

      // two projections at higher scale
      val offsetStore2 = offsetStore(512, 767)

      val p1 = "p-0960" // slice 576
      val p2 = "p-6009" // slice 640
      val p3 = "p-3039" // slice 832

      val t0 = offsetStoreClock.instant().minusSeconds(100)
      def time(step: Int) = t0.plusSeconds(step)

      // starting with 2 projections, testing 512-1023
      offsetStore1.saveOffset(OffsetPidSeqNr(TimestampOffset(time(2), Map(p1 -> 1L)), p1, 1L)).futureValue
      offsetStore1.saveOffset(OffsetPidSeqNr(TimestampOffset(time(100), Map(p3 -> 1L)), p3, 1L)).futureValue

      // scaled up to 4 projections, testing 512-767
      val latestTime = time(10)
      offsetStore2.saveOffset(OffsetPidSeqNr(TimestampOffset(latestTime, Map(p1 -> 2L)), p1, 2L)).futureValue
      offsetStore2.getState().latestTimestamp shouldBe latestTime

      // note: eventTimestampQueryClock is used by TestTimestampSourceProvider.timestampOf for timestamp of previous seqNr

      if (usingOffsetTTL) {
        // if offset TTL is configured, use expiry window (from now) to validate old timestamps

        val now = offsetStoreClock.tick(JDuration.ofSeconds(10))
        val offsetExpiry = settings.timeToLiveSettings.projections.get(projectionName).offsetTimeToLive.get.toJava

        // rejected if timestamp of previous seqNr is after expiry timestamp for this slice
        eventTimestampQueryClock.withInstant(now.minus(offsetExpiry.minusSeconds(1))) {
          offsetStore2
            .validate(backtrackingEnvelope(createEnvelope(p2, 4L, latestTime.minusSeconds(20), "event4")))
            .futureValue shouldBe RejectedBacktrackingSeqNr
        }

        // still rejected if timestamp of previous seqNr is before expiry timestamp for latest (slice not tracked)
        eventTimestampQueryClock.withInstant(now.minus(offsetExpiry.plusSeconds(1))) {
          offsetStore2
            .validate(backtrackingEnvelope(createEnvelope(p2, 4L, latestTime.minusSeconds(20), "event4")))
            .futureValue shouldBe Accepted
        }
      } else {
        // when no offset TTL expiry is configured, then always reject

        val now = offsetStoreClock.tick(JDuration.ofSeconds(10))
        val testOffsetExpiry = JDuration.ofHours(1)

        // rejected if timestamp of previous seqNr is after possible expiry timestamp (but no TTL configured)
        eventTimestampQueryClock.withInstant(now.minus(testOffsetExpiry.minusSeconds(1))) {
          offsetStore2
            .validate(backtrackingEnvelope(createEnvelope(p2, 4L, latestTime.minusSeconds(20), "event4")))
            .futureValue shouldBe RejectedBacktrackingSeqNr
        }

        // still rejected if timestamp of previous seqNr is before possible expiry timestamp (but no TTL configured)
        eventTimestampQueryClock.withInstant(now.minus(testOffsetExpiry.plusSeconds(1))) {
          offsetStore2
            .validate(backtrackingEnvelope(createEnvelope(p2, 4L, latestTime.minusSeconds(20), "event4")))
            .futureValue shouldBe RejectedBacktrackingSeqNr
        }
      }
    }

  }
}
