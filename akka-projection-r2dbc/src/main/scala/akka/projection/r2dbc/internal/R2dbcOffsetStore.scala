/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.DurableStateChange
import akka.persistence.query.TimestampOffset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.typed.PersistenceId
import akka.projection.BySlicesSourceProvider
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.internal.OffsetSerialization
import akka.projection.internal.OffsetSerialization.MultipleOffsets
import akka.projection.r2dbc.R2dbcProjectionSettings
import io.r2dbc.spi.Connection
import org.slf4j.LoggerFactory

import java.time.Clock
import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object R2dbcOffsetStore {
  type SeqNr = Long
  type Pid = String

  final case class Record(slice: Int, pid: Pid, seqNr: SeqNr, timestamp: Instant)
  final case class RecordWithOffset(
      record: Record,
      offset: TimestampOffset,
      strictSeqNr: Boolean,
      fromBacktracking: Boolean,
      fromPubSub: Boolean,
      fromSnapshot: Boolean)
  final case class RecordWithProjectionKey(record: Record, projectionKey: String)

  object State {
    val empty: State = State(Map.empty, Vector.empty, Instant.EPOCH, 0)

    def apply(records: immutable.IndexedSeq[Record]): State = {
      if (records.isEmpty) empty
      else empty.add(records)
    }
  }

  final case class State(
      byPid: Map[Pid, Record],
      latest: immutable.IndexedSeq[Record],
      oldestTimestamp: Instant,
      sizeAfterEvict: Int) {

    def size: Int = byPid.size

    def latestTimestamp: Instant =
      if (latest.isEmpty) Instant.EPOCH
      else latest.head.timestamp

    def latestOffset: Option[TimestampOffset] = {
      if (latest.isEmpty)
        None
      else
        Some(TimestampOffset(latestTimestamp, latest.map(r => r.pid -> r.seqNr).toMap))
    }

    def add(records: immutable.IndexedSeq[Record]): State = {
      records.foldLeft(this) {
        case (acc, r) =>
          val newByPid =
            acc.byPid.get(r.pid) match {
              case Some(existingRecord) =>
                if (r.seqNr > existingRecord.seqNr)
                  acc.byPid.updated(r.pid, r)
                else
                  acc.byPid // older or same seqNr
              case None =>
                acc.byPid.updated(r.pid, r)
            }

          val latestTimestamp = acc.latestTimestamp
          val newLatest =
            if (r.timestamp.isAfter(latestTimestamp)) {
              Vector(r)
            } else if (r.timestamp == latestTimestamp) {
              acc.latest.find(_.pid == r.pid) match {
                case None                 => acc.latest :+ r
                case Some(existingRecord) =>
                  // keep highest seqNr
                  if (r.seqNr >= existingRecord.seqNr)
                    acc.latest.filterNot(_.pid == r.pid) :+ r
                  else
                    acc.latest
              }
            } else {
              acc.latest // older than existing latest, keep existing latest
            }
          val newOldestTimestamp =
            if (acc.oldestTimestamp == Instant.EPOCH)
              r.timestamp // first record
            else if (r.timestamp.isBefore(acc.oldestTimestamp))
              r.timestamp
            else
              acc.oldestTimestamp // this is the normal case

          acc.copy(byPid = newByPid, latest = newLatest, oldestTimestamp = newOldestTimestamp)
      }
    }

    def isDuplicate(record: Record): Boolean = {
      byPid.get(record.pid) match {
        case Some(existingRecord) => record.seqNr <= existingRecord.seqNr
        case None                 => false
      }
    }

    def window: JDuration =
      JDuration.between(oldestTimestamp, latestTimestamp)

    private lazy val sortedByTimestamp: Vector[Record] = byPid.valuesIterator.toVector.sortBy(_.timestamp)

    lazy val latestBySlice: Vector[Record] = {
      val builder = scala.collection.mutable.Map[Int, Record]()
      sortedByTimestamp.reverseIterator.foreach { record =>
        if (!builder.contains(record.slice))
          builder.update(record.slice, record)
      }
      builder.values.toVector
    }

    def evict(until: Instant, keepNumberOfEntries: Int): State = {
      if (oldestTimestamp.isBefore(until) && size > keepNumberOfEntries) {
        val newState = State(
          sortedByTimestamp.take(size - keepNumberOfEntries).filterNot(_.timestamp.isBefore(until))
          ++ sortedByTimestamp.takeRight(keepNumberOfEntries)
          ++ latestBySlice)
        newState.copy(sizeAfterEvict = newState.size)
      } else
        this
    }

  }

  final class RejectedEnvelope(message: String) extends IllegalStateException(message)

  sealed trait Validation

  object Validation {
    case object Accepted extends Validation
    case object Duplicate extends Validation
    case object RejectedSeqNr extends Validation
    case object RejectedBacktrackingSeqNr extends Validation

    val FutureAccepted: Future[Validation] = Future.successful(Accepted)
    val FutureDuplicate: Future[Validation] = Future.successful(Duplicate)
    val FutureRejectedSeqNr: Future[Validation] = Future.successful(RejectedSeqNr)
    val FutureRejectedBacktrackingSeqNr: Future[Validation] = Future.successful(RejectedBacktrackingSeqNr)
  }

  val FutureDone: Future[Done] = Future.successful(Done)

  final case class LatestBySlice(slice: Int, pid: String, seqNr: Long)
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class R2dbcOffsetStore(
    projectionId: ProjectionId,
    sourceProvider: Option[BySlicesSourceProvider],
    system: ActorSystem[_],
    settings: R2dbcProjectionSettings,
    r2dbcExecutor: R2dbcExecutor,
    clock: Clock = Clock.systemUTC()) {

  import R2dbcOffsetStore._

  // FIXME include projectionId in all log messages
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val persistenceExt = Persistence(system)

  private val evictWindow = settings.timeWindow.plus(settings.evictInterval)

  private val offsetSerialization = new OffsetSerialization(system)
  import offsetSerialization.fromStorageRepresentation
  import offsetSerialization.toStorageRepresentation

  private val dialectName = system.settings.config.getConfig(settings.useConnectionFactory).getString("dialect")
  private val dialect =
    dialectName match {
      case "postgres"  => PostgresDialect
      case "yugabyte"  => YugabyteDialect
      case "h2"        => H2Dialect
      case "sqlserver" => SqlServerDialect
      case unknown =>
        throw new IllegalArgumentException(
          s"[$unknown] is not a dialect supported by this version of Akka Projection R2DBC")
    }
  private val dao = {
    logger.debug("Offset store [{}] created, with dialect [{}]", projectionId, dialectName)
    dialect.createOffsetStoreDao(settings, sourceProvider, system, r2dbcExecutor, projectionId)
  }

  private[projection] implicit val executionContext: ExecutionContext = system.executionContext

  // The OffsetStore instance is used by a single projectionId and there shouldn't be any concurrent
  // calls to methods that access the `state`. To detect any violations of that concurrency assumption
  // we use AtomicReference and fail if the CAS fails.
  private val state = new AtomicReference(State.empty)

  // Transient state of inflight pid -> seqNr (before they have been stored and included in `state`), which is
  // needed for at-least-once or other projections where the offset is saved afterwards. Not needed for exactly-once.
  // This can be updated concurrently with CAS retries.
  private val inflight = new AtomicReference(Map.empty[Pid, SeqNr])

  // To avoid delete requests when no new offsets have been stored since previous delete
  private val idle = new AtomicBoolean(false)

  // To trigger next deletion after in-memory eviction
  private val triggerDeletion = new AtomicBoolean(false)

  if (!settings.deleteInterval.isZero && !settings.deleteInterval.isNegative)
    system.scheduler.scheduleWithFixedDelay(
      settings.deleteInterval,
      settings.deleteInterval,
      () => deleteOldTimestampOffsets(),
      system.executionContext)

  private def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    sourceProvider match {
      case Some(timestampQuery: EventTimestampQuery) =>
        timestampQuery.timestampOf(persistenceId, sequenceNr)
      case Some(timestampQuery: akka.persistence.query.typed.javadsl.EventTimestampQuery) =>
        import scala.jdk.FutureConverters._
        import scala.jdk.OptionConverters._
        timestampQuery.timestampOf(persistenceId, sequenceNr).asScala.map(_.toScala)
      case Some(_) =>
        throw new IllegalArgumentException(
          s"Expected BySlicesSourceProvider to implement EventTimestampQuery when TimestampOffset is used.")
      case None =>
        throw new IllegalArgumentException(
          s"Expected BySlicesSourceProvider to be defined when TimestampOffset is used.")
    }
  }

  def getState(): State =
    state.get()

  def getInflight(): Map[Pid, SeqNr] =
    inflight.get()

  def getOffset[Offset](): Future[Option[Offset]] = {
    getState().latestOffset match {
      case Some(t) => Future.successful(Some(t.asInstanceOf[Offset]))
      case None    => readOffset()
    }
  }

  def readOffset[Offset](): Future[Option[Offset]] = {
    // look for TimestampOffset first since that is used by akka-persistence-r2dbc,
    // and then fall back to the other more primitive offset types
    sourceProvider match {
      case Some(_) =>
        readTimestampOffset().flatMap {
          case Some(t) => Future.successful(Some(t.asInstanceOf[Offset]))
          case None    => readPrimitiveOffset()
        }
      case None =>
        readPrimitiveOffset()
    }
  }

  private def readTimestampOffset(): Future[Option[TimestampOffset]] = {
    idle.set(false)
    val oldState = state.get()
    dao.readTimestampOffset().map { recordsWithKey =>
      val newState = State(recordsWithKey.map(_.record))
      logger.debug(
        "readTimestampOffset state with [{}] persistenceIds, oldest [{}], latest [{}]",
        newState.byPid.size,
        newState.oldestTimestamp,
        newState.latestTimestamp)
      if (!state.compareAndSet(oldState, newState))
        throw new IllegalStateException("Unexpected concurrent modification of state from readOffset.")
      clearInflight()
      if (newState == State.empty) {
        None
      } else if (moreThanOneProjectionKey(recordsWithKey)) {
        // When downscaling projection instances (changing slice distribution) there
        // is a possibility that one of the previous projection instances was further behind than the backtracking
        // window, which would cause missed events if we started from latest. In that case we use the latest
        // offset of the earliest slice range (distinct projection key).
        val latestBySliceWithKey = recordsWithKey
          .groupBy(_.record.slice)
          .map {
            case (_, records) => records.maxBy(_.record.timestamp)
          }
          .toVector
        // Only needed if there's more than one projection key within the latest offsets by slice.
        // To handle restarts after previous downscaling, and all latest are from the same instance.
        if (moreThanOneProjectionKey(latestBySliceWithKey)) {
          // Use the earliest of the latest from each projection instance (distinct projection key).
          val latestByKey =
            latestBySliceWithKey.groupBy(_.projectionKey).map {
              case (_, records) => records.maxBy(_.record.timestamp)
            }
          val earliest = latestByKey.minBy(_.record.timestamp).record
          // there could be other with same timestamp, but not important to reconstruct exactly the right `seen`
          Some(TimestampOffset(earliest.timestamp, Map(earliest.pid -> earliest.seqNr)))
        } else {
          newState.latestOffset
        }
      } else {
        newState.latestOffset
      }
    }
  }

  private def moreThanOneProjectionKey(recordsWithKey: immutable.IndexedSeq[RecordWithProjectionKey]): Boolean = {
    if (recordsWithKey.isEmpty)
      false
    else {
      val key = recordsWithKey.head.projectionKey
      recordsWithKey.exists(_.projectionKey != key)
    }
  }

  private def readPrimitiveOffset[Offset](): Future[Option[Offset]] = {
    if (settings.isOffsetTableDefined) {
      val singleOffsets = dao.readPrimitiveOffset()
      singleOffsets.map { offsets =>
        val result =
          if (offsets.isEmpty) None
          else if (offsets.forall(_.mergeable)) {
            Some(
              fromStorageRepresentation[MergeableOffset[_], Offset](MultipleOffsets(offsets.toList))
                .asInstanceOf[Offset])
          } else {
            offsets.find(_.id == projectionId).map(fromStorageRepresentation[Offset, Offset])
          }

        logger.trace("found offset [{}] for [{}]", result, projectionId)

        result
      }
    } else {
      Future.successful(None)
    }
  }

  /**
   * Like saveOffsetInTx, but in own transaction. Used by atLeastOnce.
   */
  def saveOffset(offset: OffsetPidSeqNr): Future[Done] = {
    r2dbcExecutor
      .withConnection("save offset") { conn =>
        saveOffsetInTx(conn, offset)
      }
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  /**
   * This method is used together with the users' handler code and run in same transaction.
   */
  def saveOffsetInTx(conn: Connection, offset: OffsetPidSeqNr): Future[Done] = {
    offset match {
      case OffsetPidSeqNr(t: TimestampOffset, Some((pid, seqNr))) =>
        val slice = persistenceExt.sliceForPersistenceId(pid)
        val record = Record(slice, pid, seqNr, t.timestamp)
        saveTimestampOffsetInTx(conn, Vector(record))
      case OffsetPidSeqNr(_: TimestampOffset, None) =>
        throw new IllegalArgumentException("Required EventEnvelope or DurableStateChange for TimestampOffset.")
      case _ =>
        savePrimitiveOffsetInTx(conn, offset.offset)
    }
  }

  def saveOffsets(offsets: immutable.IndexedSeq[OffsetPidSeqNr]): Future[Done] = {
    r2dbcExecutor
      .withConnection("save offsets") { conn =>
        saveOffsetsInTx(conn, offsets)
      }
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  def saveOffsetsInTx(conn: Connection, offsets: immutable.IndexedSeq[OffsetPidSeqNr]): Future[Done] = {
    if (offsets.isEmpty)
      FutureDone
    else if (offsets.head.offset.isInstanceOf[TimestampOffset]) {
      val records = offsets.map {
        case OffsetPidSeqNr(t: TimestampOffset, Some((pid, seqNr))) =>
          val slice = persistenceExt.sliceForPersistenceId(pid)
          Record(slice, pid, seqNr, t.timestamp)
        case OffsetPidSeqNr(_: TimestampOffset, None) =>
          throw new IllegalArgumentException("Required EventEnvelope or DurableStateChange for TimestampOffset.")
        case _ =>
          throw new IllegalArgumentException(
            "Mix of TimestampOffset and other offset type in same transaction isnot supported")
      }
      saveTimestampOffsetInTx(conn, records)
    } else {
      savePrimitiveOffsetInTx(conn, offsets.last.offset)
    }
  }

  private def saveTimestampOffsetInTx(conn: Connection, records: immutable.IndexedSeq[Record]): Future[Done] = {
    idle.set(false)
    val oldState = state.get()
    val filteredRecords = {
      if (records.size <= 1)
        records.filterNot(oldState.isDuplicate)
      else {
        // use last record for each pid
        records
          .groupBy(_.pid)
          .valuesIterator
          .collect {
            case recordsByPid if !oldState.isDuplicate(recordsByPid.last) => recordsByPid.last
          }
          .toVector
      }
    }
    if (filteredRecords.isEmpty) {
      FutureDone
    } else {
      val newState = oldState.add(filteredRecords)

      // accumulate some more than the timeWindow before evicting, and at least 10% increase of size
      // for testing keepNumberOfEntries = 0 is used
      val evictThresholdReached =
        if (settings.keepNumberOfEntries == 0) true else newState.size > (newState.sizeAfterEvict * 1.1).toInt
      val evictedNewState =
        if (newState.size > settings.keepNumberOfEntries && evictThresholdReached && newState.window
              .compareTo(evictWindow) > 0) {
          val evictUntil = newState.latestTimestamp.minus(settings.timeWindow)
          val s = newState.evict(evictUntil, settings.keepNumberOfEntries)
          triggerDeletion.set(true)
          logger.debug(
            "Evicted [{}] records until [{}], keeping [{}] records. Latest [{}].",
            newState.size - s.size,
            evictUntil,
            s.size,
            newState.latestTimestamp)
          s
        } else
          newState

      val offsetInserts = dao.insertTimestampOffsetInTx(conn, filteredRecords)

      offsetInserts.map { _ =>
        if (state.compareAndSet(oldState, evictedNewState))
          cleanupInflight(evictedNewState)
        else
          throw new IllegalStateException("Unexpected concurrent modification of state from saveOffset.")
        Done
      }
    }
  }
  @tailrec private def cleanupInflight(newState: State): Unit = {
    val currentInflight = getInflight()
    val newInflight =
      currentInflight.filter {
        case (inflightPid, inflightSeqNr) =>
          newState.byPid.get(inflightPid) match {
            case Some(r) => r.seqNr < inflightSeqNr
            case None    => true
          }
      }
    if (newInflight.size >= 10000) {
      throw new IllegalStateException(
        s"Too many envelopes in-flight [${newInflight.size}]. " +
        "Please report this issue at https://github.com/akka/akka-persistence-r2dbc")
    }
    if (!inflight.compareAndSet(currentInflight, newInflight))
      cleanupInflight(newState) // CAS retry, concurrent update of inflight
  }

  @tailrec private def clearInflight(): Unit = {
    val currentInflight = getInflight()
    if (!inflight.compareAndSet(currentInflight, Map.empty[Pid, SeqNr]))
      clearInflight() // CAS retry, concurrent update of inflight
  }

  private def savePrimitiveOffsetInTx[Offset](conn: Connection, offset: Offset): Future[Done] = {
    logger.trace("saving offset [{}]", offset)

    if (!settings.isOffsetTableDefined)
      Future.failed(
        new IllegalArgumentException(
          "Offset table has been disabled config 'akka.projection.r2dbc.offset-store.offset-table', " +
          s"but trying to save a non-timestamp offset [$offset]"))

    val now = Instant.now(clock)

    // FIXME can we move serialization outside the transaction?
    val storageReps = toStorageRepresentation(projectionId, offset)

    dao.updatePrimitiveOffsetInTx(conn, now, storageReps)
  }

  /**
   * The stored sequence number for a persistenceId, or 0 if unknown persistenceId.
   */
  def storedSeqNr(pid: Pid): SeqNr =
    getState().byPid.get(pid) match {
      case Some(record) => record.seqNr
      case None         => 0L
    }

  def validateAll[Envelope](envelopes: immutable.Seq[Envelope]): Future[immutable.Seq[(Envelope, Validation)]] = {
    import Validation._
    envelopes
      .foldLeft(Future.successful((getInflight(), Vector.empty[(Envelope, Validation)]))) { (acc, envelope) =>
        acc.flatMap {
          case (inflight, filteredEnvelopes) =>
            createRecordWithOffset(envelope) match {
              case Some(recordWithOffset) =>
                validate(recordWithOffset, inflight).map {
                  case Accepted =>
                    (
                      inflight.updated(recordWithOffset.record.pid, recordWithOffset.record.seqNr),
                      filteredEnvelopes :+ (envelope -> Accepted))
                  case rejected =>
                    (inflight, filteredEnvelopes :+ (envelope -> rejected))
                }
              case None =>
                Future.successful((inflight, filteredEnvelopes :+ (envelope -> Accepted)))
            }
        }
      }
      .map {
        case (_, filteredEnvelopes) =>
          filteredEnvelopes
      }
  }

  /**
   * Validate if the sequence number of the envelope is the next expected, or if the envelope
   * is a duplicate that has already been processed, or there is a gap in sequence numbers that
   * should be rejected.
   */
  def validate[Envelope](envelope: Envelope): Future[Validation] = {
    createRecordWithOffset(envelope) match {
      case Some(recordWithOffset) => validate(recordWithOffset, getInflight())
      case None                   => Validation.FutureAccepted
    }
  }

  private def validate(recordWithOffset: RecordWithOffset, currentInflight: Map[Pid, SeqNr]): Future[Validation] = {
    import Validation._
    val pid = recordWithOffset.record.pid
    val seqNr = recordWithOffset.record.seqNr
    val currentState = getState()

    val duplicate = currentState.isDuplicate(recordWithOffset.record)

    if (duplicate) {
      logger.trace("Filtering out duplicate sequence number [{}] for pid [{}]", seqNr, pid)
      FutureDuplicate
    } else if (recordWithOffset.strictSeqNr) {
      // strictSeqNr == true is for event sourced
      val prevSeqNr = currentInflight.getOrElse(pid, currentState.byPid.get(pid).map(_.seqNr).getOrElse(0L))

      def logUnexpected(): Unit = {
        if (recordWithOffset.fromPubSub)
          logger.debug(
            "Rejecting pub-sub envelope, unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
            seqNr,
            pid,
            prevSeqNr,
            recordWithOffset.offset)
        else if (!recordWithOffset.fromBacktracking)
          logger.debug(
            "Rejecting unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
            seqNr,
            pid,
            prevSeqNr,
            recordWithOffset.offset)
        else
          logger.warn(
            "Rejecting unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
            seqNr,
            pid,
            prevSeqNr,
            recordWithOffset.offset)
      }

      def logUnknown(): Unit = {
        if (recordWithOffset.fromPubSub) {
          logger.debug(
            "Rejecting pub-sub envelope, unknown sequence number [{}] for pid [{}] (might be accepted later): {}",
            seqNr,
            pid,
            recordWithOffset.offset)
        } else if (!recordWithOffset.fromBacktracking) {
          // This may happen rather frequently when using `publish-events`, after reconnecting and such.
          logger.debug(
            "Rejecting unknown sequence number [{}] for pid [{}] (might be accepted later): {}",
            seqNr,
            pid,
            recordWithOffset.offset)
        } else {
          logger.warn(
            "Rejecting unknown sequence number [{}] for pid [{}]. Offset: {}",
            seqNr,
            pid,
            recordWithOffset.offset)
        }
      }

      if (prevSeqNr > 0) {
        // expecting seqNr to be +1 of previously known
        val ok = seqNr == prevSeqNr + 1
        if (ok) {
          FutureAccepted
        } else if (seqNr <= currentInflight.getOrElse(pid, 0L)) {
          // currentInFlight contains those that have been processed or about to be processed in Flow,
          // but offset not saved yet => ok to handle as duplicate
          FutureDuplicate
        } else if (recordWithOffset.fromSnapshot) {
          // snapshots will mean we are starting from some arbitrary offset after last seen offset
          FutureAccepted
        } else if (!recordWithOffset.fromBacktracking) {
          logUnexpected()
          FutureRejectedSeqNr
        } else {
          logUnexpected()
          // This will result in projection restart (with normal configuration)
          FutureRejectedBacktrackingSeqNr
        }
      } else if (seqNr == 1) {
        // always accept first event if no other event for that pid has been seen
        FutureAccepted
      } else if (recordWithOffset.fromSnapshot) {
        // always accept starting from snapshots when there was no previous event seen
        FutureAccepted
      } else {
        // Haven't see seen this pid within the time window. Since events can be missed
        // when read at the tail we will only accept it if the event with previous seqNr has timestamp
        // before the time window of the offset store.
        // Backtracking will emit missed event again.
        timestampOf(pid, seqNr - 1).map {
          case Some(previousTimestamp) =>
            val before = currentState.latestTimestamp.minus(settings.timeWindow)
            if (previousTimestamp.isBefore(before)) {
              logger.debug(
                "Accepting envelope with pid [{}], seqNr [{}], where previous event timestamp [{}] " +
                "is before time window [{}].",
                pid,
                seqNr,
                previousTimestamp,
                before)
              Accepted
            } else if (!recordWithOffset.fromBacktracking) {
              logUnknown()
              RejectedSeqNr
            } else {
              logUnknown()
              // This will result in projection restart (with normal configuration)
              RejectedBacktrackingSeqNr
            }
          case None =>
            // previous not found, could have been deleted
            Accepted
        }
      }
    } else {
      // strictSeqNr == false is for durable state where each revision might not be visible
      val prevSeqNr = currentInflight.getOrElse(pid, currentState.byPid.get(pid).map(_.seqNr).getOrElse(0L))
      val ok = seqNr > prevSeqNr

      if (ok) {
        FutureAccepted
      } else {
        logger.trace("Filtering out earlier revision [{}] for pid [{}], previous revision [{}]", seqNr, pid, prevSeqNr)
        FutureDuplicate
      }
    }
  }

  @tailrec final def addInflight[Envelope](envelope: Envelope): Unit = {
    createRecordWithOffset(envelope) match {
      case Some(recordWithOffset) =>
        val currentInflight = getInflight()
        val newInflight = currentInflight.updated(recordWithOffset.record.pid, recordWithOffset.record.seqNr)
        if (!inflight.compareAndSet(currentInflight, newInflight))
          addInflight(envelope) // CAS retry, concurrent update of inflight
      case None =>
    }
  }

  @tailrec final def addInflights[Envelope](envelopes: immutable.Seq[Envelope]): Unit = {
    val currentInflight = getInflight()
    val entries = envelopes.iterator.map(createRecordWithOffset).collect {
      case Some(r) =>
        r.record.pid -> r.record.seqNr
    }
    val newInflight = currentInflight ++ entries
    if (!inflight.compareAndSet(currentInflight, newInflight))
      addInflights(envelopes) // CAS retry, concurrent update of inflight
  }

  def isInflight[Envelope](envelope: Envelope): Boolean = {
    createRecordWithOffset(envelope) match {
      case Some(recordWithOffset) =>
        val pid = recordWithOffset.record.pid
        val seqNr = recordWithOffset.record.seqNr
        getInflight().get(pid) match {
          case Some(`seqNr`) => true
          case _             => false
        }
      case None => true
    }
  }

  def deleteOldTimestampOffsets(): Future[Long] = {
    if (idle.getAndSet(true)) {
      // no new offsets stored since previous delete
      Future.successful(0)
    } else {
      val currentState = getState()
      if (!triggerDeletion.getAndSet(false) && currentState.window.compareTo(settings.timeWindow) < 0) {
        // it hasn't filled up the window yet
        Future.successful(0)
      } else {
        val until = currentState.latestTimestamp.minus(settings.timeWindow)

        val notInLatestBySlice = currentState.latestBySlice.collect {
          case record if record.timestamp.isBefore(until) =>
            // note that deleteOldTimestampOffsetSql already has `AND timestamp_offset < ?`
            // and that's why timestamp >= until don't have to be included here
            LatestBySlice(record.slice, record.pid, record.seqNr)
        }
        val result = dao.deleteOldTimestampOffset(until, notInLatestBySlice)
        result.failed.foreach { exc =>
          idle.set(false) // try again next tick
          logger.warn(
            "Failed to delete timestamp offset until [{}] for projection [{}]: {}",
            until,
            projectionId.id,
            exc.toString)
        }
        if (logger.isDebugEnabled)
          result.foreach { rows =>
            logger.debug(
              "Deleted [{}] timestamp offset rows until [{}] for projection [{}].",
              rows,
              until,
              projectionId.id)
          }

        result
      }
    }
  }

  /**
   * Resetting an offset. Deletes newer offsets. Used from ProjectionManagement. Doesn't update in-memory state because
   * the projection is supposed to be stopped/started for this operation.
   */
  def managementSetOffset[Offset](offset: Offset): Future[Done] = {
    offset match {
      case t: TimestampOffset =>
        r2dbcExecutor
          .withConnection("set offset") { conn =>
            deleteNewTimestampOffsetsInTx(conn, t.timestamp).flatMap { _ =>
              val records =
                if (t.seen.isEmpty) {
                  // we need some persistenceId to be able to store the new offset timestamp
                  val pid = PersistenceId("mgmt", UUID.randomUUID().toString).id
                  val slice = persistenceExt.sliceForPersistenceId(pid)
                  Vector(Record(slice, pid, seqNr = 1L, t.timestamp))
                } else
                  t.seen.iterator.map {
                    case (pid, seqNr) =>
                      val slice = persistenceExt.sliceForPersistenceId(pid)
                      Record(slice, pid, seqNr, t.timestamp)
                  }.toVector
              dao.insertTimestampOffsetInTx(conn, records)
            }
          }
          .map(_ => Done)(ExecutionContext.parasitic)

      case _ =>
        r2dbcExecutor
          .withConnection("set offset") { conn =>
            savePrimitiveOffsetInTx(conn, offset)
          }
          .map(_ => Done)(ExecutionContext.parasitic)
    }
  }

  private def deleteNewTimestampOffsetsInTx(conn: Connection, timestamp: Instant): Future[Long] = {
    val currentState = getState()
    if (timestamp.isAfter(currentState.latestTimestamp)) {
      // nothing to delete
      Future.successful(0)
    } else {
      val result = dao.deleteNewTimestampOffsetsInTx(conn, timestamp)
      if (logger.isDebugEnabled)
        result.foreach { rows =>
          logger.debug(
            "Deleted [{}] timestamp offset rows >= [{}] for projection [{}].",
            rows,
            timestamp,
            projectionId.id)
        }

      result
    }
  }

  /**
   * Deletes all offsets. Used from ProjectionManagement. Doesn't update in-memory state because the projection is
   * supposed to be stopped/started for this operation.
   */
  def managementClearOffset(): Future[Done] = {
    clearTimestampOffset().flatMap(_ => clearPrimitiveOffset())
  }

  private def clearTimestampOffset(): Future[Done] = {
    sourceProvider match {
      case Some(_) =>
        idle.set(false)
        dao
          .clearTimestampOffset()
          .map { n =>
            logger.debug(s"clearing timestamp offset for [{}] - executed statement returned [{}]", projectionId, n)
            Done
          }
      case None =>
        FutureDone
    }
  }

  private def clearPrimitiveOffset(): Future[Done] = {
    if (settings.isOffsetTableDefined) {
      dao.clearPrimitiveOffset().map { n =>
        logger.debug(s"clearing offset for [{}] - executed statement returned [{}]", projectionId, n)
        Done
      }
    } else {
      FutureDone
    }
  }

  def readManagementState(): Future[Option[ManagementState]] =
    dao.readManagementState()

  def savePaused(paused: Boolean): Future[Done] = {

    val update = dao.updateManagementState(paused, Instant.now(clock))
    (if (dialect == SqlServerDialect) {
       // workaround for https://github.com/r2dbc/r2dbc-mssql/pull/290
       update.map(_ => 1)
     } else update)
      .flatMap {
        case i if i == 1 => Future.successful(Done)
        case _ =>
          Future.failed(new RuntimeException(s"Failed to update management table for $projectionId"))
      }
  }

  private def createRecordWithOffset[Envelope](envelope: Envelope): Option[RecordWithOffset] = {
    envelope match {
      case eventEnvelope: EventEnvelope[_] if eventEnvelope.offset.isInstanceOf[TimestampOffset] =>
        val timestampOffset = eventEnvelope.offset.asInstanceOf[TimestampOffset]
        val slice = persistenceExt.sliceForPersistenceId(eventEnvelope.persistenceId)
        Some(
          RecordWithOffset(
            Record(slice, eventEnvelope.persistenceId, eventEnvelope.sequenceNr, timestampOffset.timestamp),
            timestampOffset,
            strictSeqNr = true,
            fromBacktracking = EnvelopeOrigin.fromBacktracking(eventEnvelope),
            fromPubSub = EnvelopeOrigin.fromPubSub(eventEnvelope),
            fromSnapshot = EnvelopeOrigin.fromSnapshot(eventEnvelope)))
      case change: UpdatedDurableState[_] if change.offset.isInstanceOf[TimestampOffset] =>
        val timestampOffset = change.offset.asInstanceOf[TimestampOffset]
        val slice = persistenceExt.sliceForPersistenceId(change.persistenceId)
        Some(
          RecordWithOffset(
            Record(slice, change.persistenceId, change.revision, timestampOffset.timestamp),
            timestampOffset,
            strictSeqNr = false,
            fromBacktracking = EnvelopeOrigin.fromBacktracking(change),
            fromPubSub = false,
            fromSnapshot = false))
      case change: DeletedDurableState[_] if change.offset.isInstanceOf[TimestampOffset] =>
        val timestampOffset = change.offset.asInstanceOf[TimestampOffset]
        val slice = persistenceExt.sliceForPersistenceId(change.persistenceId)
        Some(
          RecordWithOffset(
            Record(slice, change.persistenceId, change.revision, timestampOffset.timestamp),
            timestampOffset,
            strictSeqNr = false,
            fromBacktracking = false,
            fromPubSub = false,
            fromSnapshot = false))
      case change: DurableStateChange[_] if change.offset.isInstanceOf[TimestampOffset] =>
        // in case additional types are added
        throw new IllegalArgumentException(
          s"DurableStateChange [${change.getClass.getName}] not implemented yet. Please report bug at https://github.com/akka/akka-projection/issues")
      case _ => None
    }
  }

}
