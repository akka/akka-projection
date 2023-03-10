/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

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

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.DurableStateChange
import akka.persistence.query.TimestampOffset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.typed.PersistenceId
import akka.projection.BySlicesSourceProvider
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.internal.OffsetSerialization
import akka.projection.internal.OffsetSerialization.MultipleOffsets
import akka.projection.internal.OffsetSerialization.SingleOffset
import akka.projection.r2dbc.R2dbcProjectionSettings
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Statement
import org.slf4j.LoggerFactory

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
      fromPubSub: Boolean)

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
      records.foldLeft(this) { case (acc, r) =>
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
          sortedByTimestamp
            .take(size - keepNumberOfEntries)
            .filterNot(_.timestamp.isBefore(until)) ++ sortedByTimestamp.takeRight(
            keepNumberOfEntries) ++ latestBySlice)
        newState.copy(sizeAfterEvict = newState.size)
      } else
        this
    }

  }

  val FutureDone: Future[Done] = Future.successful(Done)
  val FutureTrue: Future[Boolean] = Future.successful(true)
  val FutureFalse: Future[Boolean] = Future.successful(false)
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

  private val timestampOffsetTable = settings.timestampOffsetTableWithSchema
  private val offsetTable = settings.offsetTableWithSchema
  private val managementTable = settings.managementTableWithSchema

  private[projection] implicit val executionContext: ExecutionContext = system.executionContext

  private val selectTimestampOffsetSql: String = sql"""
    SELECT slice, persistence_id, seq_nr, timestamp_offset
    FROM $timestampOffsetTable WHERE slice BETWEEN ? AND ? AND projection_name = ?"""

  private val insertTimestampOffsetSql: String = sql"""
    INSERT INTO $timestampOffsetTable
    (projection_name, projection_key, slice, persistence_id, seq_nr, timestamp_offset, timestamp_consumed)
    VALUES (?,?,?,?,?,?, transaction_timestamp())"""

  // delete less than a timestamp
  private val deleteOldTimestampOffsetSql: String = sql"""
    DELETE FROM $timestampOffsetTable WHERE slice BETWEEN ? AND ? AND projection_name = ? AND timestamp_offset < ?
    AND NOT (persistence_id || '-' || seq_nr) = ANY (?)"""

  // delete greater than or equal a timestamp
  private val deleteNewTimestampOffsetSql: String =
    sql"DELETE FROM $timestampOffsetTable WHERE slice BETWEEN ? AND ? AND projection_name = ? AND timestamp_offset >= ?"

  private val clearTimestampOffsetSql: String =
    sql"DELETE FROM $timestampOffsetTable WHERE slice BETWEEN ? AND ? AND projection_name = ?"

  private val selectOffsetSql: String =
    sql"SELECT projection_key, current_offset, manifest, mergeable FROM $offsetTable WHERE projection_name = ?"

  private val upsertOffsetSql: String = sql"""
    INSERT INTO $offsetTable
    (projection_name, projection_key, current_offset, manifest, mergeable, last_updated)
    VALUES (?,?,?,?,?,?)
    ON CONFLICT (projection_name, projection_key)
    DO UPDATE SET
    current_offset = excluded.current_offset,
    manifest = excluded.manifest,
    mergeable = excluded.mergeable,
    last_updated = excluded.last_updated"""

  private val clearOffsetSql: String =
    sql"DELETE FROM $offsetTable WHERE projection_name = ? AND projection_key = ?"

  private val readManagementStateSql = sql"""
    SELECT paused FROM $managementTable WHERE
    projection_name = ? AND
    projection_key = ? """

  val updateManagementStateSql: String = sql"""
    INSERT INTO $managementTable
    (projection_name, projection_key, paused, last_updated)
    VALUES (?,?,?,?)
    ON CONFLICT (projection_name, projection_key)
    DO UPDATE SET
    paused = excluded.paused,
    last_updated = excluded.last_updated"""

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

  system.scheduler.scheduleWithFixedDelay(
    settings.deleteInterval,
    settings.deleteInterval,
    () => deleteOldTimestampOffsets(),
    system.executionContext)

  private def timestampOffsetBySlicesSourceProvider: BySlicesSourceProvider =
    sourceProvider match {
      case Some(provider) => provider
      case None =>
        throw new IllegalArgumentException(
          s"Expected BySlicesSourceProvider to be defined when TimestampOffset is used.")
    }

  private def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    timestampOffsetBySlicesSourceProvider match {
      case timestampQuery: EventTimestampQuery =>
        timestampQuery.timestampOf(persistenceId, sequenceNr)
      case timestampQuery: akka.persistence.query.typed.javadsl.EventTimestampQuery =>
        import scala.compat.java8.FutureConverters._
        import scala.compat.java8.OptionConverters._
        timestampQuery.timestampOf(persistenceId, sequenceNr).toScala.map(_.asScala)
      case _ =>
        throw new IllegalArgumentException(
          s"Expected BySlicesSourceProvider to implement EventTimestampQuery when TimestampOffset is used.")
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
      case Some(provider) =>
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

    val (minSlice, maxSlice) = {
      sourceProvider match {
        case Some(provider) => (provider.minSlice, provider.maxSlice)
        case None           => (0, persistenceExt.numberOfSlices - 1)
      }
    }

    val recordsFut = r2dbcExecutor.select("read timestamp offset")(
      conn => {
        logger.trace("reading timestamp offset for [{}]", projectionId)
        conn
          .createStatement(selectTimestampOffsetSql)
          .bind(0, minSlice)
          .bind(1, maxSlice)
          .bind(2, projectionId.name)
      },
      row => {
        val slice = row.get("slice", classOf[java.lang.Integer])
        val pid = row.get("persistence_id", classOf[String])
        val seqNr = row.get("seq_nr", classOf[java.lang.Long])
        val timestamp = row.get("timestamp_offset", classOf[Instant])
        Record(slice, pid, seqNr, timestamp)
      })
    recordsFut.map { records =>
      val newState = State(records)
      logger.debugN(
        "readTimestampOffset state with [{}] persistenceIds, oldest [{}], latest [{}]",
        newState.byPid.size,
        newState.oldestTimestamp,
        newState.latestTimestamp)
      if (!state.compareAndSet(oldState, newState))
        throw new IllegalStateException("Unexpected concurrent modification of state from readOffset.")
      clearInflight()
      if (newState == State.empty) {
        None
      } else {
        newState.latestOffset
      }
    }
  }

  private def readPrimitiveOffset[Offset](): Future[Option[Offset]] = {
    if (settings.isOffsetTableDefined) {
      val singleOffsets = r2dbcExecutor.select("read offset")(
        conn => {
          logger.trace("reading offset for [{}]", projectionId)
          conn
            .createStatement(selectOffsetSql)
            .bind(0, projectionId.name)
        },
        row => {
          val offsetStr = row.get("current_offset", classOf[String])
          val manifest = row.get("manifest", classOf[String])
          val mergeable = row.get("mergeable", classOf[java.lang.Boolean])
          val key = row.get("projection_key", classOf[String])

          val adaptedProjectionId = ProjectionId(projectionId.name, key)
          SingleOffset(adaptedProjectionId, manifest, offsetStr, mergeable)
        })

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

        logger.trace2("found offset [{}] for [{}]", result, projectionId)

        result
      }
    } else {
      Future.successful(None)
    }
  }

  /**
   * Like saveOffsetInTx, but in own transaction. Used by atLeastOnce.
   */
  def saveOffset[Offset](offset: Offset): Future[Done] = {
    r2dbcExecutor
      .withConnection("save offset") { conn =>
        saveOffsetInTx(conn, offset)
      }
      .map(_ => Done)(ExecutionContexts.parasitic)
  }

  /**
   * This method is used together with the users' handler code and run in same transaction.
   */
  def saveOffsetInTx[Offset](conn: Connection, offset: Offset): Future[Done] = {
    offset match {
      case t: TimestampOffset =>
        // TODO possible perf improvement to optimize for the normal case of 1 record
        val records = t.seen.map { case (pid, seqNr) =>
          val slice = persistenceExt.sliceForPersistenceId(pid)
          Record(slice, pid, seqNr, t.timestamp)
        }.toVector
        saveTimestampOffsetInTx(conn, records)
      case _ =>
        savePrimitiveOffsetInTx(conn, offset)
    }
  }

  def saveOffsets[Offset](offsets: immutable.IndexedSeq[Offset]): Future[Done] = {
    r2dbcExecutor
      .withConnection("save offsets") { conn =>
        saveOffsetsInTx(conn, offsets)
      }
      .map(_ => Done)(ExecutionContexts.parasitic)
  }

  def saveOffsetsInTx[Offset](conn: Connection, offsets: immutable.IndexedSeq[Offset]): Future[Done] = {
    if (offsets.exists(_.isInstanceOf[TimestampOffset])) {
      val records = offsets.flatMap {
        case t: TimestampOffset =>
          t.seen.map { case (pid, seqNr) =>
            val slice = persistenceExt.sliceForPersistenceId(pid)
            Record(slice, pid, seqNr, t.timestamp)
          }
        case _ =>
          Nil
      }
      saveTimestampOffsetInTx(conn, records)
    } else {
      savePrimitiveOffsetInTx(conn, offsets.last)
    }
  }

  private def saveTimestampOffsetInTx[Offset](conn: Connection, records: immutable.IndexedSeq[Record]): Future[Done] = {
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
          logger.debugN(
            "Evicted [{}] records until [{}], keeping [{}] records. Latest [{}].",
            newState.size - s.size,
            evictUntil,
            s.size,
            newState.latestTimestamp)
          s
        } else
          newState

      val offsetInserts = insertTimestampOffsetInTx(conn, filteredRecords)

      offsetInserts.map { _ =>
        if (state.compareAndSet(oldState, evictedNewState))
          cleanupInflight(evictedNewState)
        else
          throw new IllegalStateException("Unexpected concurrent modification of state from saveOffset.")
        Done
      }
    }
  }

  private def insertTimestampOffsetInTx(conn: Connection, records: immutable.IndexedSeq[Record]): Future[Long] = {
    def bindRecord(stmt: Statement, record: Record): Statement = {
      val slice = persistenceExt.sliceForPersistenceId(record.pid)
      val minSlice = timestampOffsetBySlicesSourceProvider.minSlice
      val maxSlice = timestampOffsetBySlicesSourceProvider.maxSlice
      if (slice < minSlice || slice > maxSlice)
        throw new IllegalArgumentException(
          s"This offset store [$projectionId] manages slices " +
          s"[$minSlice - $maxSlice] but received slice [$slice] for persistenceId [${record.pid}]")

      stmt
        .bind(0, projectionId.name)
        .bind(1, projectionId.key)
        .bind(2, slice)
        .bind(3, record.pid)
        .bind(4, record.seqNr)
        .bind(5, record.timestamp)
    }

    require(records.nonEmpty)

    logger.trace2("saving timestamp offset [{}], {}", records.last.timestamp, records)

    val statement = conn.createStatement(insertTimestampOffsetSql)

    if (records.size == 1) {
      val boundStatement = bindRecord(statement, records.head)
      R2dbcExecutor.updateOneInTx(boundStatement)
    } else {
      // TODO Try Batch without bind parameters for better performance. Risk of sql injection for these parameters is low.
      val boundStatement =
        records.foldLeft(statement) { (stmt, rec) =>
          stmt.add()
          bindRecord(stmt, rec)
        }
      R2dbcExecutor.updateBatchInTx(boundStatement)
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
        case _ => true
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

    val now = Instant.now(clock).toEpochMilli

    // FIXME can we move serialization outside the transaction?
    val storageReps = toStorageRepresentation(projectionId, offset)

    def upsertStmt(singleOffset: SingleOffset): Statement = {
      conn
        .createStatement(upsertOffsetSql)
        .bind(0, singleOffset.id.name)
        .bind(1, singleOffset.id.key)
        .bind(2, singleOffset.offsetStr)
        .bind(3, singleOffset.manifest)
        .bind(4, java.lang.Boolean.valueOf(singleOffset.mergeable))
        .bind(5, now)
    }

    val statements = storageReps match {
      case single: SingleOffset  => Vector(upsertStmt(single))
      case MultipleOffsets(many) => many.map(upsertStmt).toVector
    }

    R2dbcExecutor.updateInTx(statements).map(_ => Done)(ExecutionContexts.parasitic)
  }

  def isDuplicate(record: Record): Boolean =
    getState().isDuplicate(record)

  def filterAccepted[Envelope](envelopes: immutable.Seq[Envelope]): Future[immutable.Seq[Envelope]] = {
    envelopes
      .foldLeft(Future.successful(getInflight(), Vector.empty[Envelope])) { (acc, envelope) =>
        acc.flatMap { case (inflight, filteredEnvelopes) =>
          createRecordWithOffset(envelope) match {
            case Some(recordWithOffset) =>
              isAccepted(recordWithOffset, inflight).map {
                case true =>
                  (
                    inflight.updated(recordWithOffset.record.pid, recordWithOffset.record.seqNr),
                    filteredEnvelopes :+ envelope)
                case false =>
                  (inflight, filteredEnvelopes)
              }
            case None =>
              Future.successful((inflight, filteredEnvelopes :+ envelope))
          }
        }
      }
      .map { case (_, filteredEnvelopes) =>
        filteredEnvelopes
      }
  }

  def isAccepted[Envelope](envelope: Envelope): Future[Boolean] = {
    createRecordWithOffset(envelope) match {
      case Some(recordWithOffset) => isAccepted(recordWithOffset, getInflight())
      case None                   => FutureTrue
    }
  }

  private def isAccepted[Envelope](
      recordWithOffset: RecordWithOffset,
      currentInflight: Map[Pid, SeqNr]): Future[Boolean] = {
    val pid = recordWithOffset.record.pid
    val seqNr = recordWithOffset.record.seqNr
    val currentState = getState()

    val duplicate = isDuplicate(recordWithOffset.record)

    if (duplicate) {
      logger.trace("Filtering out duplicate sequence number [{}] for pid [{}]", seqNr, pid)
      FutureFalse
    } else if (recordWithOffset.strictSeqNr) {
      // strictSeqNr == true is for event sourced
      val prevSeqNr = currentInflight.getOrElse(pid, currentState.byPid.get(pid).map(_.seqNr).getOrElse(0L))

      def logUnexpected(): Unit = {
        if (recordWithOffset.fromPubSub)
          logger.debugN(
            "Rejecting pub-sub envelope, unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
            seqNr,
            pid,
            prevSeqNr,
            recordWithOffset.offset)
        else if (!recordWithOffset.fromBacktracking)
          logger.debugN(
            "Rejecting unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
            seqNr,
            pid,
            prevSeqNr,
            recordWithOffset.offset)
        else
          logger.warnN(
            "Rejecting unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
            seqNr,
            pid,
            prevSeqNr,
            recordWithOffset.offset)
      }

      def logUnknown(): Unit = {
        if (recordWithOffset.fromPubSub) {
          logger.debugN(
            "Rejecting pub-sub envelope, unknown sequence number [{}] for pid [{}] (might be accepted later): {}",
            seqNr,
            pid,
            recordWithOffset.offset)
        } else if (!recordWithOffset.fromBacktracking) {
          // This may happen rather frequently when using `publish-events`, after reconnecting and such.
          logger.debugN(
            "Rejecting unknown sequence number [{}] for pid [{}] (might be accepted later): {}",
            seqNr,
            pid,
            recordWithOffset.offset)
        } else {
          logger.warnN(
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
          FutureTrue
        } else if (seqNr <= currentInflight.getOrElse(pid, 0L)) {
          // currentInFlight contains those that have been processed or about to be processed in Flow,
          // but offset not saved yet => ok to handle as duplicate
          FutureFalse
        } else if (!recordWithOffset.fromBacktracking) {
          logUnexpected()
          FutureFalse
        } else {
          logUnexpected()
          // This will result in projection restart (with normal configuration)
          Future.failed(
            new IllegalStateException(
              s"Rejected envelope from backtracking, persistenceId [$pid], seqNr [$seqNr] " +
              "due to unexpected sequence number. " +
              "Please report this issue at https://github.com/akka/akka-persistence-r2dbc"))
        }
      } else if (seqNr == 1) {
        // always accept first event if no other event for that pid has been seen
        FutureTrue
      } else {
        // Haven't see seen this pid within the time window. Since events can be missed
        // when read at the tail we will only accept it if the event with previous seqNr has timestamp
        // before the time window of the offset store.
        // Backtracking will emit missed event again.
        timestampOf(pid, seqNr - 1).map {
          case Some(previousTimestamp) =>
            val before = currentState.latestTimestamp.minus(settings.timeWindow)
            if (previousTimestamp.isBefore(before)) {
              logger.debugN(
                "Accepting envelope with pid [{}], seqNr [{}], where previous event timestamp [{}] " +
                "is before time window [{}].",
                pid,
                seqNr,
                previousTimestamp,
                before)
              true
            } else if (!recordWithOffset.fromBacktracking) {
              logUnknown()
              false
            } else {
              logUnknown()
              // This will result in projection restart (with normal configuration)
              throw new IllegalStateException(
                s"Rejected envelope from backtracking, persistenceId [$pid], seqNr [$seqNr], " +
                "due to unknown sequence number. " +
                "Please report this issue at https://github.com/akka/akka-persistence-r2dbc")
            }
          case None =>
            // previous not found, could have been deleted
            true
        }
      }
    } else {
      // strictSeqNr == false is for durable state where each revision might not be visible
      val prevSeqNr = currentInflight.getOrElse(pid, currentState.byPid.get(pid).map(_.seqNr).getOrElse(0L))
      val ok = seqNr > prevSeqNr

      if (ok) {
        FutureTrue
      } else {
        logger.traceN("Filtering out earlier revision [{}] for pid [{}], previous revision [{}]", seqNr, pid, prevSeqNr)
        FutureFalse
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
    val entries = envelopes.iterator.map(createRecordWithOffset).collect { case Some(r) =>
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
      if (currentState.size <= settings.keepNumberOfEntries || currentState.window.compareTo(settings.timeWindow) < 0) {
        // it hasn't filled up the window yet
        Future.successful(0)
      } else {
        val until = currentState.latestTimestamp.minus(settings.timeWindow)
        val minSlice = timestampOffsetBySlicesSourceProvider.minSlice
        val maxSlice = timestampOffsetBySlicesSourceProvider.maxSlice
        val notInLatestBySlice = currentState.latestBySlice.map { record =>
          s"${record.pid}-${record.seqNr}"
        }.toArray

        val result = r2dbcExecutor.updateOne("delete old timestamp offset") { conn =>
          conn
            .createStatement(deleteOldTimestampOffsetSql)
            .bind(0, minSlice)
            .bind(1, maxSlice)
            .bind(2, projectionId.name)
            .bind(3, until)
            .bind(4, notInLatestBySlice)
        }

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
            logger.debugN(
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
                  t.seen.iterator.map { case (pid, seqNr) =>
                    val slice = persistenceExt.sliceForPersistenceId(pid)
                    Record(slice, pid, seqNr, t.timestamp)
                  }.toVector
              insertTimestampOffsetInTx(conn, records)
            }
          }
          .map(_ => Done)(ExecutionContexts.parasitic)

      case _ =>
        r2dbcExecutor
          .withConnection("set offset") { conn =>
            savePrimitiveOffsetInTx(conn, offset)
          }
          .map(_ => Done)(ExecutionContexts.parasitic)
    }
  }

  private def deleteNewTimestampOffsetsInTx(conn: Connection, timestamp: Instant): Future[Long] = {
    val currentState = getState()
    if (timestamp.isAfter(currentState.latestTimestamp)) {
      // nothing to delete
      Future.successful(0)
    } else {
      val minSlice = timestampOffsetBySlicesSourceProvider.minSlice
      val maxSlice = timestampOffsetBySlicesSourceProvider.maxSlice
      val result = R2dbcExecutor.updateOneInTx(
        conn
          .createStatement(deleteNewTimestampOffsetSql)
          .bind(0, minSlice)
          .bind(1, maxSlice)
          .bind(2, projectionId.name)
          .bind(3, timestamp))

      if (logger.isDebugEnabled)
        result.foreach { rows =>
          logger.debugN(
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
        r2dbcExecutor
          .updateOne("clear timestamp offset") { conn =>
            val minSlice = timestampOffsetBySlicesSourceProvider.minSlice
            val maxSlice = timestampOffsetBySlicesSourceProvider.maxSlice
            logger.debug("clearing timestamp offset for [{}]", projectionId)
            conn
              .createStatement(clearTimestampOffsetSql)
              .bind(0, minSlice)
              .bind(1, maxSlice)
              .bind(2, projectionId.name)
          }
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
      r2dbcExecutor
        .updateOne("clear offset") { conn =>
          logger.debug("clearing offset for [{}]", projectionId)
          conn
            .createStatement(clearOffsetSql)
            .bind(0, projectionId.name)
            .bind(1, projectionId.key)
        }
        .map { n =>
          logger.debug(s"clearing offset for [{}] - executed statement returned [{}]", projectionId, n)
          Done
        }
    } else {
      FutureDone
    }
  }

  def readManagementState(): Future[Option[ManagementState]] = {
    def createStatement(connection: Connection) =
      connection
        .createStatement(readManagementStateSql)
        .bind(0, projectionId.name)
        .bind(1, projectionId.key)

    r2dbcExecutor
      .selectOne("read management state")(
        conn => createStatement(conn),
        row => ManagementState(row.get[java.lang.Boolean]("paused", classOf[java.lang.Boolean])))
  }

  def savePaused(paused: Boolean): Future[Done] = {
    r2dbcExecutor
      .updateOne("update management state") { conn =>
        conn
          .createStatement(updateManagementStateSql)
          .bind(0, projectionId.name)
          .bind(1, projectionId.key)
          .bind(2, paused)
          .bind(3, Instant.now(clock).toEpochMilli)
      }
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
            fromPubSub = EnvelopeOrigin.fromPubSub(eventEnvelope)))
      case change: UpdatedDurableState[_] if change.offset.isInstanceOf[TimestampOffset] =>
        val timestampOffset = change.offset.asInstanceOf[TimestampOffset]
        val slice = persistenceExt.sliceForPersistenceId(change.persistenceId)
        Some(
          RecordWithOffset(
            Record(slice, change.persistenceId, change.revision, timestampOffset.timestamp),
            timestampOffset,
            strictSeqNr = false,
            fromBacktracking = EnvelopeOrigin.fromBacktracking(change),
            fromPubSub = false))
      case change: DeletedDurableState[_] if change.offset.isInstanceOf[TimestampOffset] =>
        val timestampOffset = change.offset.asInstanceOf[TimestampOffset]
        val slice = persistenceExt.sliceForPersistenceId(change.persistenceId)
        Some(
          RecordWithOffset(
            Record(slice, change.persistenceId, change.revision, timestampOffset.timestamp),
            timestampOffset,
            strictSeqNr = false,
            fromBacktracking = false,
            fromPubSub = false))
      case change: DurableStateChange[_] if change.offset.isInstanceOf[TimestampOffset] =>
        // in case additional types are added
        throw new IllegalArgumentException(
          s"DurableStateChange [${change.getClass.getName}] not implemented yet. Please report bug at https://github.com/akka/akka-persistence-r2dbc/issues")
      case _ => None
    }
  }

}
