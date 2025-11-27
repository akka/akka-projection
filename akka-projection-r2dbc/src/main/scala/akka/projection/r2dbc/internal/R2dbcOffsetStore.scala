/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.r2dbc.internal

import java.lang.Boolean.FALSE
import java.lang.Boolean.TRUE
import java.time.Clock
import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.immutable.TreeSet
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.Cancellable
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
import akka.projection.AllowSeqNrGapsMetadata
import akka.projection.BySlicesSourceProvider
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.internal.OffsetSerialization
import akka.projection.internal.OffsetSerialization.MultipleOffsets
import akka.projection.r2dbc.R2dbcProjectionSettings
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import io.r2dbc.spi.Connection
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object R2dbcOffsetStore {
  type SeqNr = Long
  type Pid = String

  final case class Record(slice: Int, pid: Pid, seqNr: SeqNr, timestamp: Instant) extends Ordered[Record] {
    override def compare(that: Record): Int =
      timestamp.compareTo(that.timestamp) match {
        case 0 =>
          Integer.compare(slice, that.slice) match {
            case 0 =>
              pid.compareTo(that.pid) match {
                case 0      => java.lang.Long.compare(seqNr, that.seqNr)
                case result => result
              }
            case result => result
          }
        case result => result
      }
  }

  final case class RecordWithOffset(
      record: Record,
      offset: TimestampOffset,
      strictSeqNr: Boolean,
      fromBacktracking: Boolean,
      fromPubSub: Boolean,
      fromSnapshot: Boolean)
  final case class RecordWithProjectionKey(record: Record, projectionKey: String)

  object State {
    val empty: State = State(Map.empty[Pid, Record], Map.empty[Int, TreeSet[Record]])

    def apply(records: immutable.IndexedSeq[Record], acceptResetAfter: Option[JDuration] = None): State = {
      if (records.isEmpty) empty
      else empty.add(records, acceptResetAfter)
    }
  }

  final case class State(byPid: Map[Pid, Record], bySliceSorted: Map[Int, TreeSet[Record]]) {

    def size: Int = byPid.size

    def latestTimestamp: Instant = {
      if (bySliceSorted.isEmpty)
        Instant.EPOCH
      else
        bySliceSorted.valuesIterator.map(_.last.timestamp).max
    }

    def latestOffset: Option[TimestampOffset] = {
      if (bySliceSorted.isEmpty) {
        None
      } else {
        val t = latestTimestamp
        val latest =
          bySliceSorted.valuesIterator.flatMap { records =>
            if (records.nonEmpty && records.last.timestamp == t)
              records.toVector.reverseIterator.takeWhile(_.timestamp == t).toVector
            else
              Vector.empty
          }.toVector

        val seen = latest.foldLeft(Map.empty[Pid, SeqNr]) {
          case (acc, record) =>
            acc.get(record.pid) match {
              case None => acc.updated(record.pid, record.seqNr)
              case Some(existing) =>
                if (record.seqNr > existing) acc.updated(record.pid, record.seqNr)
                else acc
            }
        }

        Some(TimestampOffset(t, seen))
      }
    }

    def add(records: Iterable[Record], acceptResetAfter: Option[JDuration] = None): State = {
      records.foldLeft(this) {
        case (acc, r) =>
          val sorted = acc.bySliceSorted.getOrElse(r.slice, TreeSet.empty[Record])
          acc.byPid.get(r.pid) match {
            case Some(existingRecord) =>
              if ((r.seqNr > existingRecord.seqNr &&
                  acceptResetAfter.forall(acceptAfter => !Validation.acceptReset(existingRecord, r, acceptAfter))) ||
                  (r.seqNr <= existingRecord.seqNr &&
                  acceptResetAfter.exists(acceptAfter => Validation.acceptReset(r, existingRecord, acceptAfter))))
                acc.copy(
                  byPid = acc.byPid.updated(r.pid, r),
                  bySliceSorted = acc.bySliceSorted.updated(r.slice, sorted - existingRecord + r))
              else
                acc // older or same seqNr
            case None =>
              acc.copy(
                byPid = acc.byPid.updated(r.pid, r),
                bySliceSorted = acc.bySliceSorted.updated(r.slice, sorted + r))
          }
      }
    }

    def contains(pid: Pid): Boolean =
      byPid.contains(pid)

    def isDuplicate(record: Record, acceptResetAfter: Option[JDuration]): Boolean = {
      byPid.get(record.pid) match {
        case Some(existingRecord) =>
          record.seqNr <= existingRecord.seqNr &&
          acceptResetAfter.forall(acceptAfter => !Validation.acceptReset(record, existingRecord, acceptAfter))
        case None => false
      }
    }

    def evict(slice: Int, timeWindow: JDuration, ableToEvictRecord: Record => Boolean): State = {
      val recordsSortedByTimestamp = bySliceSorted.getOrElse(slice, TreeSet.empty[Record])
      if (recordsSortedByTimestamp.isEmpty) {
        this
      } else {
        val until = recordsSortedByTimestamp.last.timestamp.minus(timeWindow)
        val filtered = {
          // Records comparing >= this record by recordOrdering will definitely be kept,
          // Records comparing < this record by recordOrdering are subject to eviction
          // Slice will be equal, and pid will compare lexicographically less than any valid pid
          val untilRecord = Record(slice, "", 0, until)
          // this will always keep at least one, latest per slice
          val newerRecords = recordsSortedByTimestamp.rangeFrom(untilRecord) // inclusive of until
          val olderRecords = recordsSortedByTimestamp.rangeUntil(untilRecord) // exclusive of until
          val filteredOlder = olderRecords.filterNot(ableToEvictRecord)

          if (filteredOlder.size == olderRecords.size) recordsSortedByTimestamp
          else newerRecords.union(filteredOlder)
        }

        // adding back filtered is linear in the size of filtered, but so is checking if we're able to evict
        if (filtered eq recordsSortedByTimestamp) {
          this
        } else {
          val byPidOtherSlices = byPid.filterNot { case (_, r) => r.slice == slice }
          val bySliceOtherSlices = bySliceSorted - slice
          copy(byPid = byPidOtherSlices, bySliceSorted = bySliceOtherSlices)
            .add(filtered)
        }
      }
    }

    override def toString: String = {
      val sb = new StringBuilder
      sb.append("State(")
      bySliceSorted.toVector.sortBy(_._1).foreach {
        case (slice, records) =>
          sb.append("slice ").append(slice).append(": ")
          records.foreach { r =>
            sb.append("[").append(r.pid).append("->").append(r.seqNr).append(" ").append(r.timestamp).append("] ")
          }
      }
      sb.append(")")
      sb.toString
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

    def acceptReset(candidate: Record, reference: Record, acceptAfter: JDuration): Boolean =
      candidate.timestamp.isAfter(reference.timestamp.plus(acceptAfter))
  }

  val FutureDone: Future[Done] = Future.successful(Done)

  final case class LatestBySlice(slice: Int, pid: String, seqNr: Long)

  final case class ScheduledTasks(adoptForeignOffsets: Option[Cancellable], deleteOffsets: Option[Cancellable]) {
    def cancel(): Unit = {
      adoptForeignOffsets.foreach(_.cancel())
      deleteOffsets.foreach(_.cancel())
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class R2dbcOffsetStore(
    projectionId: ProjectionId,
    sourceProvider: Option[BySlicesSourceProvider],
    system: ActorSystem[_],
    val settings: R2dbcProjectionSettings,
    r2dbcExecutor: R2dbcExecutor,
    clock: Clock = Clock.systemUTC()) {

  import R2dbcOffsetStore._

  private val persistenceExt = Persistence(system)

  private val (minSlice, maxSlice) = {
    sourceProvider match {
      case Some(provider) => (provider.minSlice, provider.maxSlice)
      case None           => (0, persistenceExt.numberOfSlices - 1)
    }
  }

  private val logger = LoggerFactory.getLogger(this.getClass)
  val logPrefix = s"${projectionId.name} [$minSlice-$maxSlice]:"

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
  val dao: OffsetStoreDao = {
    logger.debug("{} Offset store created, with dialect [{}]", logPrefix, dialectName)
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
  private val triggerDeletionPerSlice = new ConcurrentHashMap[Int, java.lang.Boolean]

  // Foreign offsets (latest by slice offsets from other projection keys) that should be adopted when passed in time.
  // Contains remaining offsets to adopt. Sorted by timestamp. Can be updated concurrently with CAS retries.
  private val foreignOffsets = new AtomicReference(Seq.empty[RecordWithProjectionKey])

  // The latest timestamp as seen by validation or saving offsets. For determining when to update foreign offsets.
  // Can be updated concurrently with CAS retries.
  private val latestSeen = new AtomicReference(Instant.EPOCH)

  private val adoptingForeignOffsets = !settings.adoptInterval.isZero && !settings.adoptInterval.isNegative

  private val scheduledTasks = new AtomicReference[ScheduledTasks](ScheduledTasks(None, None))

  @tailrec
  private def scheduleTasks(): Unit = {
    val existingTasks = scheduledTasks.get()
    existingTasks.cancel()

    val foreignOffsets =
      if (adoptingForeignOffsets)
        Some(
          system.scheduler.scheduleWithFixedDelay(
            settings.adoptInterval,
            settings.adoptInterval,
            () => adoptForeignOffsets(),
            system.executionContext))
      else None

    val deleteOffsets =
      if (!settings.deleteInterval.isZero && !settings.deleteInterval.isNegative)
        Some(
          system.scheduler.scheduleWithFixedDelay(
            settings.deleteInterval,
            settings.deleteInterval,
            () => deleteOldTimestampOffsets(),
            system.executionContext))
      else
        None

    val newTasks = ScheduledTasks(foreignOffsets, deleteOffsets)
    if (!scheduledTasks.compareAndSet(existingTasks, newTasks)) {
      newTasks.cancel()
      scheduleTasks() // CAS retry, concurrent update
    }
  }

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
          s"$logPrefix Expected BySlicesSourceProvider to implement EventTimestampQuery when TimestampOffset is used.")
      case None =>
        throw new IllegalArgumentException(
          s"$logPrefix Expected BySlicesSourceProvider to be defined when TimestampOffset is used.")
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

  private def dumpState(s: State, flight: Map[Pid, SeqNr]): String = {
    s"$s inFlight [${flight.map { case (pid, seqNr) => s"$pid->$seqNr" }.mkString(",")}]"
  }

  def readOffset[Offset](): Future[Option[Offset]] = {
    scheduleTasks()

    // look for TimestampOffset first since that is used by akka-persistence-r2dbc,
    // and then fall back to the other more primitive offset types
    sourceProvider match {
      case Some(_) =>
        readTimestampOffset().flatMap {
          case Some(t) => Future.successful(Some(t.asInstanceOf[Offset]))
          case None =>
            settings.acceptWhenPreviousTimestampBefore match {
              case Some(t) => Future.successful(Some(TimestampOffset(t, Map.empty).asInstanceOf[Offset]))
              case None    => readPrimitiveOffset()
            }
        }
      case None =>
        readPrimitiveOffset()
    }
  }

  private def readTimestampOffset(): Future[Option[TimestampOffset]] = {
    implicit val sys = system // for implicit stream materializer
    triggerDeletionPerSlice.clear()
    val oldState = state.get()

    val recordsWithKeyFut =
      Source(minSlice to maxSlice)
        .mapAsyncUnordered(settings.offsetSliceReadParallelism) { slice =>
          dao.readTimestampOffset(slice)
        }
        .mapConcat(identity)
        .runWith(Sink.seq)
        .map(_.toVector)(ExecutionContext.parasitic)

    recordsWithKeyFut.map { recordsWithKey =>
      clearInflight()
      clearForeignOffsets()
      clearLatestSeen()

      val newState = {
        val s = State(recordsWithKey.map(_.record), settings.acceptSequenceNumberResetAfter)
        (minSlice to maxSlice).foldLeft(s) {
          case (acc, slice) => acc.evict(slice, settings.timeWindow, _ => true)
        }
      }

      val startOffset =
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
            if (adoptingForeignOffsets) {
              val foreignOffsets = latestBySliceWithKey
                .filter(_.projectionKey != projectionId.key)
                .sortBy(_.record.timestamp)
              setForeignOffsets(foreignOffsets)
            }
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

      logger.debug(
        "{} readTimestampOffset state with [{}] persistenceIds, latest [{}], start offset [{}]",
        logPrefix,
        newState.byPid.size,
        newState.latestTimestamp,
        startOffset)

      if (!state.compareAndSet(oldState, newState))
        throw new IllegalStateException(
          s"$logPrefix Unexpected concurrent modification of state from readOffset. " +
          s"${dumpState(oldState, getInflight())}")

      startOffset
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

        logger.trace("{} found offset [{}]", logPrefix, result)

        result
      }
    } else {
      Future.successful(None)
    }
  }

  def load(pid: Pid): Future[State] = {
    val oldState = state.get()
    if (oldState.contains(pid))
      Future.successful(oldState)
    else {
      val slice = persistenceExt.sliceForPersistenceId(pid)
      logger.trace("{} load [{}]", logPrefix, pid)
      dao.readTimestampOffset(slice, pid).flatMap {
        case Some(record) =>
          val newState = oldState.add(Vector(record), settings.acceptSequenceNumberResetAfter)
          if (state.compareAndSet(oldState, newState))
            Future.successful(newState)
          else
            load(pid) // CAS retry, concurrent update
        case None => Future.successful(oldState)
      }
    }
  }

  def load(pids: IndexedSeq[Pid]): Future[State] = {
    val oldState = state.get()
    val pidsToLoad = pids.filterNot(oldState.contains)
    if (pidsToLoad.isEmpty)
      Future.successful(oldState)
    else {
      val loadedRecords = pidsToLoad.map { pid =>
        val slice = persistenceExt.sliceForPersistenceId(pid)
        logger.trace("{} load [{}]", logPrefix, pid)
        dao.readTimestampOffset(slice, pid)
      }
      Future.sequence(loadedRecords).flatMap { records =>
        val newState = oldState.add(records.flatten, settings.acceptSequenceNumberResetAfter)
        if (state.compareAndSet(oldState, newState))
          Future.successful(newState)
        else
          load(pids) // CAS retry, concurrent update
      }
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
        saveTimestampOffsetInTx(conn, Vector(record), canBeConcurrent = true)
      case OffsetPidSeqNr(_: TimestampOffset, None) =>
        throw new IllegalArgumentException(
          s"$logPrefix Required EventEnvelope or DurableStateChange for TimestampOffset.")
      case _ =>
        savePrimitiveOffsetInTx(conn, offset.offset)
    }
  }

  def saveOffsets(offsets: immutable.IndexedSeq[OffsetPidSeqNr]): Future[Done] = {
    r2dbcExecutor
      .withConnection("save offsets") { conn =>
        saveOffsetsInTx(conn, offsets, canBeConcurrent = true)
      }
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  def saveOffsetsInTx(conn: Connection, offsets: immutable.IndexedSeq[OffsetPidSeqNr]): Future[Done] =
    saveOffsetsInTx(conn, offsets, canBeConcurrent = false)

  private def saveOffsetsInTx(
      conn: Connection,
      offsets: immutable.IndexedSeq[OffsetPidSeqNr],
      canBeConcurrent: Boolean): Future[Done] = {
    if (offsets.isEmpty)
      FutureDone
    else if (offsets.head.offset.isInstanceOf[TimestampOffset]) {
      val records = offsets.map {
        case OffsetPidSeqNr(t: TimestampOffset, Some((pid, seqNr))) =>
          val slice = persistenceExt.sliceForPersistenceId(pid)
          Record(slice, pid, seqNr, t.timestamp)
        case OffsetPidSeqNr(_: TimestampOffset, None) =>
          throw new IllegalArgumentException(
            s"$logPrefix Required EventEnvelope or DurableStateChange for TimestampOffset.")
        case _ =>
          throw new IllegalArgumentException(
            s"$logPrefix Mix of TimestampOffset and other offset type in same transaction is not supported")
      }
      saveTimestampOffsetInTx(conn, records, canBeConcurrent)
    } else {
      savePrimitiveOffsetInTx(conn, offsets.last.offset)
    }
  }

  private def saveTimestampOffsetInTx(
      conn: Connection,
      records: immutable.IndexedSeq[Record],
      canBeConcurrent: Boolean): Future[Done] = {
    load(records.map(_.pid)).flatMap { oldState =>
      val filteredRecords = {
        if (records.size <= 1)
          records.filterNot(record => oldState.isDuplicate(record, settings.acceptSequenceNumberResetAfter))
        else {
          // use last record for each pid
          records
            .groupBy(_.pid)
            .valuesIterator
            .collect {
              case recordsByPid if !oldState.isDuplicate(recordsByPid.last, settings.acceptSequenceNumberResetAfter) =>
                recordsByPid.last
            }
            .toVector
        }
      }
      if (hasForeignOffsets() && records.nonEmpty) {
        val latestTimestamp =
          if (records.size == 1) records.head.timestamp
          else records.maxBy(_.timestamp).timestamp
        updateLatestSeen(latestTimestamp)
      }
      if (filteredRecords.isEmpty) {
        logger.trace("{} save offsets, all duplicates", logPrefix)
        FutureDone
      } else {
        val offsetInserts = dao.insertTimestampOffsetInTx(conn, filteredRecords)

        val slices =
          if (filteredRecords.size == 1) Set(filteredRecords.head.slice)
          else filteredRecords.iterator.map(_.slice).toSet

        def addRecordsAndEvict(old: State): State = {
          val newState = old.add(filteredRecords, settings.acceptSequenceNumberResetAfter)

          val currentInflight = getInflight()
          slices.foldLeft(newState) {
            case (s, slice) =>
              s.evict(
                slice,
                settings.timeWindow,
                // Only persistence IDs that aren't inflight are evictable,
                // if only so that those persistence IDs can be removed from
                // inflight... in the absence of further records from that
                // persistence ID, the next store will evict (further records
                // would make that persistence ID recent enough to not be evicted)
                record => !currentInflight.contains(record.pid))
          }
        }

        def compareAndSwapRetry(): Future[Done] = {
          load(records.map(_.pid)).flatMap { old =>
            val evictedNewState = addRecordsAndEvict(old)
            if (state.compareAndSet(old, evictedNewState)) {
              slices.foreach(s => triggerDeletionPerSlice.put(s, TRUE))
              cleanupInflight(evictedNewState)
              FutureDone
            } else {
              // concurrent update
              compareAndSwapRetry()
            }
          }
        }

        val evictedNewState = addRecordsAndEvict(oldState)

        if (logger.isTraceEnabled)
          offsetInserts.failed.foreach { exc =>
            logger.trace(
              "{} save offsets failed [{}]: {}",
              logPrefix,
              filteredRecords.iterator.map(r => s"${r.pid} -> ${r.seqNr}").mkString(", "),
              exc.toString)
          }

        offsetInserts.flatMap { _ =>
          if (logger.isTraceEnabled)
            logger.trace(
              "{} save offsets [{}]",
              logPrefix,
              filteredRecords.iterator.map(r => s"${r.pid} -> ${r.seqNr}").mkString(", "))
          if (state.compareAndSet(oldState, evictedNewState)) {
            slices.foreach(s => triggerDeletionPerSlice.put(s, TRUE))
            cleanupInflight(evictedNewState)
            FutureDone
          } else {
            // concurrent update
            // the offsets have already been successfully stored within this transaction so we can't
            // retry entire saveTimestampOffsetInTx because that would result in unique constraint violation
            if (canBeConcurrent) {
              compareAndSwapRetry()
            } else
              throw new IllegalStateException(
                s"$logPrefix Unexpected concurrent modification of state from saveOffset. " +
                s"${dumpState(evictedNewState, getInflight())}")
          }

        }
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
        s"$logPrefix Too many envelopes in-flight [${newInflight.size}]. " +
        "Please report this issue at https://github.com/akka/akka-projection " +
        s"${dumpState(newState, newInflight)}")
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
    logger.trace("{} saving offset [{}]", logPrefix, offset)

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
  def storedSeqNr(pid: Pid): Future[SeqNr] = {
    getState().byPid.get(pid) match {
      case Some(record) => Future.successful(record.seqNr)
      case None =>
        val slice = persistenceExt.sliceForPersistenceId(pid)
        dao.readTimestampOffset(slice, pid).map {
          case Some(record) => record.seqNr
          case None         => 0L
        }
    }
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

    load(pid).flatMap { currentState =>
      val duplicate = currentState.isDuplicate(recordWithOffset.record, settings.acceptSequenceNumberResetAfter)

      if (duplicate) {
        logger.trace("{} Filtering out duplicate sequence number [{}] for pid [{}]", logPrefix, seqNr, pid)
        // also move latest seen forward, for adopting foreign offsets on replay of duplicates
        if (hasForeignOffsets()) updateLatestSeen(recordWithOffset.offset.timestamp)
        FutureDuplicate
      } else if (recordWithOffset.strictSeqNr) {
        // strictSeqNr == true is for event sourced
        val prevSeqNr = currentInflight.getOrElse(pid, currentState.byPid.get(pid).map(_.seqNr).getOrElse(0L))

        def logUnexpected(): Unit = {
          if (recordWithOffset.fromPubSub)
            logger.debug(
              "{} Rejecting pub-sub envelope, unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
              logPrefix,
              seqNr,
              pid,
              prevSeqNr,
              recordWithOffset.offset)
          else if (!recordWithOffset.fromBacktracking)
            logger.debug(
              "{} Rejecting unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
              logPrefix,
              seqNr,
              pid,
              prevSeqNr,
              recordWithOffset.offset)
          else
            logger.warn(
              "{} Rejecting unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
              logPrefix,
              seqNr,
              pid,
              prevSeqNr,
              recordWithOffset.offset)
        }

        if (prevSeqNr > 0) {
          // expecting seqNr to be +1 of previously known
          val ok = seqNr == prevSeqNr + 1
          // detect reset of sequence number (for example, if events were deleted and then pid was recreated)
          val seqNrReset = (seqNr == 1) && currentState.byPid.get(pid).exists { existingRecord =>
              settings.acceptSequenceNumberResetAfter.exists { acceptAfter =>
                Validation.acceptReset(recordWithOffset.record, existingRecord, acceptAfter)
              }
            }
          if (ok || seqNrReset) {
            FutureAccepted
          } else if (seqNr <= currentInflight.getOrElse(pid, 0L)) {
            // currentInFlight contains those that have been processed or about to be processed in Flow,
            // but offset not saved yet => ok to handle as duplicate
            logger.trace(
              "{} Filtering out duplicate in-flight sequence number [{}] for pid [{}]",
              logPrefix,
              seqNr,
              pid)
            FutureDuplicate
          } else if (recordWithOffset.fromSnapshot) {
            // snapshots will mean we are starting from some arbitrary offset after last seen offset
            FutureAccepted
          } else if (!recordWithOffset.fromBacktracking) {
            // Rejected will trigger replay of missed events, if replay-on-rejected-sequence-numbers is enabled
            // and SourceProvider supports it.
            logUnexpected()
            FutureRejectedSeqNr
          } else {
            // Rejected will trigger replay of missed events, if replay-on-rejected-sequence-numbers is enabled
            // and SourceProvider supports it.
            // Otherwise this will result in projection restart (with normal configuration).
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
          validateEventTimestamp(currentState, recordWithOffset)
        }
      } else {
        // strictSeqNr == false is for durable state where each revision might not be visible
        val prevSeqNr = currentInflight.getOrElse(pid, currentState.byPid.get(pid).map(_.seqNr).getOrElse(0L))
        val ok = seqNr > prevSeqNr

        if (ok) {
          FutureAccepted
        } else {
          logger.trace(
            "{} Filtering out earlier revision [{}] for pid [{}], previous revision [{}]",
            logPrefix,
            seqNr,
            pid,
            prevSeqNr)
          FutureDuplicate
        }
      }
    }
  }

  private def validateEventTimestamp(currentState: State, recordWithOffset: RecordWithOffset) = {
    import Validation._
    val pid = recordWithOffset.record.pid
    val seqNr = recordWithOffset.record.seqNr
    val slice = recordWithOffset.record.slice

    // Haven't seen this pid in the time window (or lazy loaded from the database).
    // Only accept it if the event with previous seqNr is outside the deletion window (for tracked slices).
    timestampOf(pid, seqNr - 1).map {
      case Some(previousTimestamp) =>
        val acceptBefore =
          max(currentState.bySliceSorted.get(slice).map { bySliceSorted =>
            bySliceSorted.last.timestamp.minus(settings.deleteAfter)
          }, settings.acceptWhenPreviousTimestampBefore)

        if (acceptBefore.exists(timestamp => previousTimestamp.isBefore(timestamp))) {
          logger.debug(
            "{} Accepting envelope with pid [{}], seqNr [{}], where previous event timestamp [{}] " +
            "is before deletion window timestamp [{}] for slice [{}].",
            logPrefix,
            pid,
            seqNr,
            previousTimestamp,
            acceptBefore.fold("none")(_.toString),
            slice)
          Accepted
        } else if (recordWithOffset.fromPubSub) {
          // Rejected will trigger replay of missed events, if replay-on-rejected-sequence-numbers is enabled
          // and SourceProvider supports it.
          logger.debug(
            "{} Rejecting pub-sub envelope, unknown sequence number [{}] for pid [{}] (might be accepted later): {}",
            logPrefix,
            seqNr,
            pid,
            recordWithOffset.offset)
          RejectedSeqNr
        } else if (recordWithOffset.fromBacktracking) {
          // Rejected will trigger replay of missed events, if replay-on-rejected-sequence-numbers is enabled
          // and SourceProvider supports it.
          logger.warn(
            "{} Rejecting unknown sequence number [{}] for pid [{}]. Offset: {}, where previous event timestamp [{}] " +
            "is after deletion window timestamp [{}] for slice [{}].",
            logPrefix,
            seqNr,
            pid,
            recordWithOffset.offset,
            previousTimestamp,
            acceptBefore.fold("none")(_.toString),
            slice)
          RejectedBacktrackingSeqNr
        } else {
          // This may happen rather frequently when using `publish-events`, after reconnecting and such.
          logger.debug(
            "{} Rejecting unknown sequence number [{}] for pid [{}] (might be accepted later): {}",
            logPrefix,
            seqNr,
            pid,
            recordWithOffset.offset)
          // Rejected will trigger replay of missed events, if replay-on-rejected-sequence-numbers is enabled
          // and SourceProvider supports it.
          // Backtracking will emit missed event again.
          RejectedSeqNr
        }
      case None =>
        // previous not found, could have been deleted
        logger.debug(
          "{} Accepting envelope with pid [{}], seqNr [{}], where previous event not found.",
          logPrefix,
          pid,
          seqNr)
        Accepted
    }
  }

  private def max(a: Option[Instant], b: Option[Instant]): Option[Instant] = {
    (a, b) match {
      case (None, None)             => None
      case (s: Some[Instant], None) => s
      case (None, s: Some[Instant]) => s
      case (Some(x), Some(y))       => Some(if (x.isBefore(y)) x else y)
    }
  }

  @tailrec final def addInflight[Envelope](envelope: Envelope): Unit = {
    createRecordWithOffset(envelope) match {
      case Some(recordWithOffset) =>
        if (logger.isTraceEnabled)
          logger.trace(
            "{} Envelope in flight, about to be processed. slice [{}], pid [{}], seqNr [{}], timestamp [{}]",
            logPrefix,
            recordWithOffset.record.slice,
            recordWithOffset.record.pid,
            recordWithOffset.record.seqNr,
            recordWithOffset.record.timestamp)

        val pid = recordWithOffset.record.pid
        val seqNr = recordWithOffset.record.seqNr
        val currentInflight = getInflight()
        val updateNeeded =
          currentInflight.get(pid) match {
            case Some(currentInFlightSeqNr) =>
              currentInFlightSeqNr < seqNr
            case None =>
              true
          }
        if (updateNeeded) {
          val newInflight = currentInflight.updated(pid, seqNr)
          if (!inflight.compareAndSet(currentInflight, newInflight)) {
            addInflight(envelope) // CAS retry, concurrent update of inflight
          }
        }
      case None =>
    }
  }

  def isInflight[Envelope](envelope: Envelope): Boolean = {
    createRecordWithOffset(envelope) match {
      case Some(recordWithOffset) =>
        val pid = recordWithOffset.record.pid
        val seqNr = recordWithOffset.record.seqNr
        getInflight().get(pid) match {
          case Some(currentInFlightSeqNr) =>
            seqNr <= currentInFlightSeqNr
          case None =>
            false
        }
      case None => true
    }
  }

  def deleteOldTimestampOffsets(): Future[Long] = {
    // This is running in the background, so fine to progress slowly one slice at a time
    def loop(slice: Int, count: Long): Future[Long] = {
      if (slice > maxSlice)
        Future.successful(count)
      else
        deleteOldTimestampOffsets(slice).flatMap { c =>
          loop(slice + 1, count + c)
        }
    }

    val result = loop(minSlice, 0L)

    if (logger.isDebugEnabled)
      result.foreach { rows =>
        logger.debug("{} Deleted [{}] timestamp offset rows", logPrefix, rows)
      }

    result
  }

  def deleteOldTimestampOffsets(slice: Int): Future[Long] = {
    val triggerDeletion = triggerDeletionPerSlice.put(slice, FALSE)
    val currentState = getState()
    if ((triggerDeletion == null || triggerDeletion == TRUE) && currentState.bySliceSorted.contains(slice)) {
      val latest = currentState.bySliceSorted(slice).last
      val until = latest.timestamp.minus(settings.deleteAfter)

      // note that deleteOldTimestampOffsetSql already has `AND timestamp_offset < ?`,
      // which means that the latest for this slice will not be deleted
      val result = dao.deleteOldTimestampOffset(slice, until)
      result.failed.foreach { exc =>
        triggerDeletionPerSlice.put(slice, TRUE) // try again next tick
        logger.warn(
          "{} Failed to delete timestamp offset, slice [{}], until [{}] : {}",
          logPrefix,
          slice,
          until,
          exc.toString)
      }
      if (logger.isDebugEnabled)
        result.foreach { rows =>
          logger.debug("{} Deleted [{}] timestamp offset rows, slice [{}], until [{}]", logPrefix, rows, slice, until)
        }

      result
    } else {
      // no new offsets stored since previous delete
      Future.successful(0L)
    }
  }

  def getForeignOffsets(): Seq[RecordWithProjectionKey] =
    foreignOffsets.get()

  def hasForeignOffsets(): Boolean =
    adoptingForeignOffsets && getForeignOffsets().nonEmpty

  @tailrec private def setForeignOffsets(records: Seq[RecordWithProjectionKey]): Unit = {
    val currentForeignOffsets = getForeignOffsets()
    if (!foreignOffsets.compareAndSet(currentForeignOffsets, records))
      setForeignOffsets(records) // CAS retry, concurrent update of foreignOffsets
  }

  // return the adoptable foreign offsets up to the latest timestamp, and set to the remaining foreign offsets
  @tailrec private def takeAdoptableForeignOffsets(latestTimestamp: Instant): Seq[RecordWithProjectionKey] = {
    val currentForeignOffsets = getForeignOffsets()
    val adoptable = currentForeignOffsets.takeWhile(_.record.timestamp.compareTo(latestTimestamp) <= 0)
    if (adoptable.isEmpty) Seq.empty
    else {
      val remainingForeignOffsets = currentForeignOffsets.drop(adoptable.size)
      if (foreignOffsets.compareAndSet(currentForeignOffsets, remainingForeignOffsets)) adoptable
      else takeAdoptableForeignOffsets(latestTimestamp) // CAS retry, concurrent update of foreignOffsets
    }
  }

  private def clearForeignOffsets(): Unit = setForeignOffsets(Seq.empty)

  def getLatestSeen(): Instant =
    latestSeen.get()

  @tailrec private def updateLatestSeen(instant: Instant): Unit = {
    val currentLatestSeen = getLatestSeen()
    if (instant.isAfter(currentLatestSeen)) {
      if (!latestSeen.compareAndSet(currentLatestSeen, instant))
        updateLatestSeen(instant) // CAS retry, concurrent update of latestSeen
    }
  }

  @tailrec private def clearLatestSeen(): Unit = {
    val currentLatestSeen = getLatestSeen()
    if (!latestSeen.compareAndSet(currentLatestSeen, Instant.EPOCH))
      clearLatestSeen() // CAS retry, concurrent update of latestSeen
  }

  def adoptForeignOffsets(): Future[Long] = {
    if (!hasForeignOffsets()) {
      scheduledTasks.get.adoptForeignOffsets.foreach(_.cancel())
      Future.successful(0)
    } else {
      val latestTimestamp = getLatestSeen()
      val adoptableRecords = takeAdoptableForeignOffsets(latestTimestamp)
      if (!hasForeignOffsets()) scheduledTasks.get.adoptForeignOffsets.foreach(_.cancel())
      val adoptableLatestBySlice = adoptableRecords.map { adoptable =>
        LatestBySlice(adoptable.record.slice, adoptable.record.pid, adoptable.record.seqNr)
      }
      dao.adoptTimestampOffsets(adoptableLatestBySlice)
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
          logger.debug("{} Deleted [{}] timestamp offset rows >= [{}]", logPrefix, rows, timestamp)
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
        triggerDeletionPerSlice.clear()
        dao
          .clearTimestampOffset()
          .map { n =>
            logger.debug("{} clearing timestamp offsets - executed statement returned [{}]", logPrefix, n)
            Done
          }
      case None =>
        FutureDone
    }
  }

  private def clearPrimitiveOffset(): Future[Done] = {
    if (settings.isOffsetTableDefined) {
      dao.clearPrimitiveOffset().map { n =>
        logger.debug("{} clearing offsets - executed statement returned [{}]", logPrefix, n)
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
        // Allow gaps if envelope has AllowSeqNrGapsMetadata
        val strictSeqNr = eventEnvelope.metadata[AllowSeqNrGapsMetadata.type].isEmpty
        Some(
          RecordWithOffset(
            Record(slice, eventEnvelope.persistenceId, eventEnvelope.sequenceNr, timestampOffset.timestamp),
            timestampOffset,
            strictSeqNr,
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
          s"$logPrefix DurableStateChange [${change.getClass.getName}] not implemented yet. Please report bug at https://github.com/akka/akka-projection/issues")
      case _ => None
    }
  }

  def stop(): Unit = {
    scheduledTasks.get.cancel()
    scheduledTasks.set(ScheduledTasks(None, None))
  }

}
