/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.internal

import java.time.Clock
import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable.TreeSet
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.DurationConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.DurableStateChange
import akka.persistence.query.TimestampOffset
import akka.persistence.query.TimestampOffsetBySlice
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.projection.AllowSeqNrGapsMetadata
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.internal.ManagementState
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.Materializer.matFromSystem
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object DynamoDBOffsetStore {
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

  object State {
    val empty: State = State(Map.empty, Map.empty, Map.empty)

    def apply(offsetBySlice: Map[Int, TimestampOffset]): State =
      if (offsetBySlice.isEmpty) empty
      else new State(Map.empty, Map.empty, offsetBySlice)
  }

  final case class State(
      byPid: Map[Pid, Record],
      bySliceSorted: Map[Int, TreeSet[Record]],
      offsetBySlice: Map[Int, TimestampOffset]) {

    def size: Int = byPid.size

    def latestTimestamp: Instant =
      if (offsetBySlice.isEmpty) Instant.EPOCH
      else offsetBySlice.valuesIterator.map(_.timestamp).max

    def latestOffset: TimestampOffset =
      if (offsetBySlice.isEmpty) TimestampOffset.Zero
      else offsetBySlice.valuesIterator.maxBy(_.timestamp)

    def add(records: Iterable[Record], acceptResetAfter: Option[JDuration] = None): State = {
      records.foldLeft(this) {
        case (acc, r) =>
          val newOffsetBySlice =
            acc.offsetBySlice.get(r.slice) match {
              case Some(existing) =>
                if (r.timestamp.isAfter(existing.timestamp))
                  acc.offsetBySlice.updated(r.slice, TimestampOffset(r.timestamp, Map(r.pid -> r.seqNr)))
                else if (r.timestamp == existing.timestamp)
                  acc.offsetBySlice
                    .updated(r.slice, TimestampOffset(r.timestamp, existing.seen.updated(r.pid, r.seqNr)))
                else
                  acc.offsetBySlice
              case None =>
                acc.offsetBySlice.updated(r.slice, TimestampOffset(r.timestamp, Map(r.pid -> r.seqNr)))
            }

          val sorted = acc.bySliceSorted.getOrElse(r.slice, TreeSet.empty[Record])
          acc.byPid.get(r.pid) match {
            case Some(existingRecord) =>
              if ((r.seqNr > existingRecord.seqNr &&
                  acceptResetAfter.forall(acceptAfter => !Validation.acceptReset(existingRecord, r, acceptAfter))) ||
                  (r.seqNr <= existingRecord.seqNr &&
                  acceptResetAfter.exists(acceptAfter => Validation.acceptReset(r, existingRecord, acceptAfter))))
                acc.copy(
                  byPid = acc.byPid.updated(r.pid, r),
                  bySliceSorted = acc.bySliceSorted.updated(r.slice, sorted - existingRecord + r),
                  offsetBySlice = newOffsetBySlice)
              else
                // older or same seqNr
                acc.copy(offsetBySlice = newOffsetBySlice)
            case None =>
              acc.copy(
                byPid = acc.byPid.updated(r.pid, r),
                bySliceSorted = acc.bySliceSorted.updated(r.slice, sorted + r),
                offsetBySlice = newOffsetBySlice)
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
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class DynamoDBOffsetStore(
    projectionId: ProjectionId,
    sourceProvider: Option[BySlicesSourceProvider],
    system: ActorSystem[_],
    val settings: DynamoDBProjectionSettings,
    client: DynamoDbAsyncClient,
    clock: Clock = Clock.systemUTC()) {

  import DynamoDBOffsetStore._

  private val persistenceExt = Persistence(system)

  private val (minSlice: Int, maxSlice: Int) = sourceProvider match {
    case Some(s) => s.minSlice -> s.maxSlice
    case None    => 0 -> (persistenceExt.numberOfSlices - 1)
  }

  private val logger = LoggerFactory.getLogger(this.getClass)
  val logPrefix = s"${projectionId.name} [$minSlice-$maxSlice]:"

  private val dao = new OffsetStoreDao(system, settings, projectionId, client)

  private val offsetExpiry =
    settings.timeToLiveSettings.projections.get(projectionId.name).offsetTimeToLive.map(_.toJava)

  private[projection] implicit val executionContext: ExecutionContext = system.executionContext

  // The OffsetStore instance is used by a single projectionId and there shouldn't be many concurrent
  // calls to methods that access the `state`, but for example validate (load) may be concurrent
  // with save. Therefore, this state can be updated concurrently with CAS retries.
  private val state = new AtomicReference(State.empty)

  // Transient state of inflight pid -> seqNr (before they have been stored and included in `state`), which is
  // needed for at-least-once or other projections where the offset is saved afterwards. Not needed for exactly-once.
  // This can be updated concurrently with CAS retries.
  private val inflight = new AtomicReference(Map.empty[Pid, SeqNr])

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

  // This is used by projection management and returns latest offset
  def getOffset[Offset](): Future[Option[Offset]] = {
    // FIXME in r2dbc this will reload via readOffset if no state
    Future.successful(Some(getState().latestOffset).map(_.asInstanceOf[Offset]))
  }

  private def dumpState(s: State, flight: Map[Pid, SeqNr]): String = {
    s"$s inFlight [${flight.map { case (pid, seqNr) => s"$pid->$seqNr" }.mkString(",")}]"
  }

  def readOffset[Offset](): Future[Option[Offset]] = {
    // look for TimestampOffset first since that is used by akka-persistence-dynamodb,
    // and then fall back to the other more primitive offset types
    sourceProvider match {
      case Some(_) =>
        readTimestampOffset().map { offsetBySlice =>
          if (offsetBySlice.offsets.isEmpty) None
          else Some(offsetBySlice.asInstanceOf[Offset])
        }(ExecutionContext.parasitic)
      case None =>
        // FIXME primitive offsets not supported, maybe we can change the sourceProvider parameter
        throw new IllegalStateException(
          s"$logPrefix BySlicesSourceProvider is required. Primitive offsets not supported.")
    }
  }

  private def readTimestampOffset(): Future[TimestampOffsetBySlice] = {
    implicit val sys = system // for implicit stream materializer
    val oldState = state.get()
    // retrieve latest timestamp for each slice, and use the earliest
    val offsetBySliceFut =
      Source(minSlice to maxSlice)
        .mapAsyncUnordered(settings.offsetSliceReadParallelism) { slice =>
          dao
            .loadTimestampOffset(slice)
            .map { optTimestampOffset =>
              optTimestampOffset.map { timestampOffset => slice -> timestampOffset }
            }(ExecutionContext.parasitic)
        }
        .mapConcat(identity)
        .runWith(Sink.fold(Map.empty[Int, TimestampOffset]) { (offsetMap, sliceAndOffset: (Int, TimestampOffset)) =>
          offsetMap + sliceAndOffset
        })

    offsetBySliceFut.map { offsetBySlice =>
      val newState = State(offsetBySlice)

      if (!state.compareAndSet(oldState, newState))
        throw new IllegalStateException(
          s"$logPrefix Unexpected concurrent modification of state from readOffset. " +
          s"${dumpState(oldState, getInflight())}")
      clearInflight()
      if (offsetBySlice.isEmpty) {
        logger.debug("{} readTimestampOffset no stored offset", logPrefix)
        TimestampOffsetBySlice.empty
      } else {
        if (logger.isDebugEnabled)
          logger.debug(
            "{} readTimestampOffset state with [{}] persistenceIds, timestamp per slice [{}]",
            logPrefix,
            newState.byPid.size,
            offsetBySlice.iterator.map { case (slice, offset) => s"$slice -> ${offset.timestamp}" }.mkString(", "))

        TimestampOffsetBySlice(offsetBySlice)
      }
    }
  }

  def load(pid: Pid): Future[State] = {
    val oldState = state.get()
    if (oldState.contains(pid))
      Future.successful(oldState)
    else {
      val slice = persistenceExt.sliceForPersistenceId(pid)
      dao.loadSequenceNumber(slice, pid).flatMap {
        case Some(record) =>
          logger.trace("{} loaded pid [{}], seqNr [{}]", logPrefix, pid, record.seqNr)
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
        dao.loadSequenceNumber(slice, pid)
      }
      Future.sequence(loadedRecords).flatMap { records =>
        if (logger.isTraceEnabled) {
          records.flatten.foreach { record =>
            logger.trace("{} loaded pid [{}], seqNr [{}]", logPrefix, record.pid, record.seqNr)
          }
        }
        val newState = oldState.add(records.flatten, settings.acceptSequenceNumberResetAfter)
        if (state.compareAndSet(oldState, newState))
          Future.successful(newState)
        else
          load(pids) // CAS retry, concurrent update
      }
    }
  }

  def saveOffset(offset: OffsetPidSeqNr): Future[Done] =
    saveOffsets(Vector(offset))

  def saveOffsets(offsets: IndexedSeq[OffsetPidSeqNr]): Future[Done] =
    storeOffsets(offsets, dao.storeSequenceNumbers, canBeConcurrent = true)

  def transactSaveOffset(writeItems: Iterable[TransactWriteItem], offset: OffsetPidSeqNr): Future[Done] =
    transactSaveOffsets(writeItems, Vector(offset))

  def transactSaveOffsets(writeItems: Iterable[TransactWriteItem], offsets: IndexedSeq[OffsetPidSeqNr]): Future[Done] =
    storeOffsets(offsets, dao.transactStoreSequenceNumbers(writeItems), canBeConcurrent = false)

  private def storeOffsets(
      offsets: IndexedSeq[OffsetPidSeqNr],
      storeSequenceNumbers: IndexedSeq[Record] => Future[Done],
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
      storeTimestampOffsets(records, storeSequenceNumbers, canBeConcurrent)
    } else {
      throw new IllegalStateException(s"$logPrefix TimestampOffset is required. Primitive offsets not supported.")
    }
  }

  private def storeTimestampOffsets(
      records: IndexedSeq[Record],
      storeSequenceNumbers: IndexedSeq[Record] => Future[Done],
      canBeConcurrent: Boolean): Future[Done] = {
    load(records.map(_.pid)).flatMap { oldState =>
      val filteredRecords =
        if (records.size <= 1)
          records.filterNot(record => oldState.isDuplicate(record, settings.acceptSequenceNumberResetAfter))
        else {
          // Can assume (given other projection guarantees) that records for the same pid
          // have montonically increasing sequence numbers
          records
            .groupBy(_.pid)
            .valuesIterator
            .collect {
              case recordsByPid if !oldState.isDuplicate(recordsByPid.last, settings.acceptSequenceNumberResetAfter) =>
                recordsByPid.last
            }
            .toVector
        }

      if (filteredRecords.isEmpty) {
        FutureDone
      } else {
        val newState = oldState.add(filteredRecords, settings.acceptSequenceNumberResetAfter)

        val slices =
          if (filteredRecords.size == 1) Set(filteredRecords.head.slice)
          else filteredRecords.iterator.map(_.slice).toSet

        val currentInflight = getInflight()
        val evictedNewState = slices.foldLeft(newState) {
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

        // FIXME we probably don't have to store the latest offset per slice all the time, but can
        //       accumulate some changes and flush on size/time.
        val changedOffsetBySlice = slices.flatMap { slice =>
          val newOffset = newState.offsetBySlice(slice)
          val oldOffset = oldState.offsetBySlice.getOrElse(slice, TimestampOffset.Zero)
          if (newOffset.timestamp.isBefore(oldOffset.timestamp)) None
          else Some(slice -> newOffset)
        }.toMap

        storeSequenceNumbers(filteredRecords).flatMap { _ =>
          val storeOffsetsResult =
            if (changedOffsetBySlice.isEmpty)
              FutureDone
            else
              dao.storeTimestampOffsets(changedOffsetBySlice)
          storeOffsetsResult.flatMap { _ =>
            if (logger.isTraceEnabled) {
              logger.trace(
                "{} Stored offsets, timestamps per slice [{}], seqNr per pid [{}]",
                logPrefix,
                changedOffsetBySlice.iterator
                  .map { case (slice, offset) => s"$slice -> ${offset.timestamp}" }
                  .mkString(", "),
                filteredRecords.iterator.map(record => s"${record.pid} -> ${record.seqNr}").mkString(", "))
            }
            if (state.compareAndSet(oldState, evictedNewState)) {
              cleanupInflight(evictedNewState)
              FutureDone
            } else { // concurrent update
              if (canBeConcurrent) storeTimestampOffsets(records, storeSequenceNumbers, canBeConcurrent) // CAS retry
              else
                throw new IllegalStateException(
                  s"$logPrefix Unexpected concurrent modification of state in save offsets.")
            }
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
        s"Too many envelopes in-flight [${newInflight.size}]. " +
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

  /**
   * The stored sequence number for a persistenceId, or 0 if unknown persistenceId.
   */
  def storedSeqNr(pid: Pid): Future[SeqNr] = {
    getState().byPid.get(pid) match {
      case Some(record) => Future.successful(record.seqNr)
      case None =>
        val slice = persistenceExt.sliceForPersistenceId(pid)
        dao.loadSequenceNumber(slice, pid).map {
          case Some(record) => record.seqNr
          case None         => 0L
        }
    }
  }

  def validateAll[Envelope](envelopes: Seq[Envelope]): Future[Seq[(Envelope, Validation)]] = {
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
   * Validate if the sequence number of the envelope is the next expected, or if the envelope is a duplicate that has
   * already been processed, or there is a gap in sequence numbers that should be rejected.
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
            FutureDuplicate
          } else if (recordWithOffset.fromSnapshot) {
            // snapshots will mean we are starting from some arbitrary offset after last seen offset
            FutureAccepted
          } else if (!recordWithOffset.fromBacktracking) {
            logUnexpected()
            // Rejected will trigger replay of missed events, if replay-on-rejected-sequence-numbers is enabled
            // and SourceProvider supports it.
            FutureRejectedSeqNr
          } else {
            logUnexpected()
            // Rejected will trigger replay of missed events, if replay-on-rejected-sequence-numbers is enabled
            // and SourceProvider supports it.
            // Otherwise this will result in projection restart (with normal configuration).
            FutureRejectedBacktrackingSeqNr
          }
        } else if (seqNr == 1) {
          // always accept first event if no other event for that pid has been seen
          FutureAccepted
        } else if (recordWithOffset.fromSnapshot) {
          // always accept starting from snapshots when there was no previous event seen
          FutureAccepted
        } else {
          validateEventTimestamp(recordWithOffset)
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

  private def validateEventTimestamp(recordWithOffset: RecordWithOffset) = {
    import Validation._
    val pid = recordWithOffset.record.pid
    val seqNr = recordWithOffset.record.seqNr

    // Haven't seen this pid in the time window (or lazy loaded from the database).
    // Only accept if the event with previous seqNr is outside the TTL expiry window (if configured).
    timestampOf(pid, seqNr - 1).map {
      case Some(previousTimestamp) =>
        val acceptBefore = offsetExpiry.map { expiry =>
          val now = clock.instant() // expiry is from when an offset was written, consider the window from now
          now.minus(expiry)
        }

        if (acceptBefore.exists(timestamp => previousTimestamp.isBefore(timestamp))) {
          logger.debug(
            "Accepting envelope with pid [{}], seqNr [{}], where previous event timestamp [{}] " +
            "is before TTL expiry window timestamp [{}].",
            pid,
            seqNr,
            previousTimestamp,
            acceptBefore.fold("none")(_.toString))
          Accepted
        } else if (recordWithOffset.fromPubSub) {
          logger.debug(
            "Rejecting pub-sub envelope, unknown sequence number [{}] for pid [{}] (might be accepted later): {}",
            seqNr,
            pid,
            recordWithOffset.offset)
          RejectedSeqNr
        } else if (recordWithOffset.fromBacktracking) {
          // This will result in projection restart (with normal configuration)
          logger.warn(
            "Rejecting unknown sequence number [{}] for pid [{}]. Offset: {}, where previous event timestamp [{}] " +
            "is after TTL expiry window timestamp [{}].",
            seqNr,
            pid,
            recordWithOffset.offset,
            previousTimestamp,
            acceptBefore.fold("none")(_.toString))
          // Rejected will trigger replay of missed events, if replay-on-rejected-sequence-numbers is enabled
          // and SourceProvider supports it.
          RejectedBacktrackingSeqNr
        } else {
          // This may happen rather frequently when using `publish-events`, after reconnecting and such.
          logger.debug(
            "Rejecting unknown sequence number [{}] for pid [{}] (might be accepted later): {}",
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
        logger.debug("Accepting envelope with pid [{}], seqNr [{}], where previous event not found.", pid, seqNr)
        Accepted
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
        val currentInflight = getInflight()
        val newInflight = currentInflight.updated(recordWithOffset.record.pid, recordWithOffset.record.seqNr)
        if (!inflight.compareAndSet(currentInflight, newInflight))
          addInflight(envelope) // CAS retry, concurrent update of inflight
      case None =>
    }
  }

  @tailrec final def addInflights[Envelope](envelopes: Seq[Envelope]): Unit = {
    val currentInflight = getInflight()
    val entries = envelopes.iterator.map(createRecordWithOffset).collect {
      case Some(r) =>
        if (logger.isTraceEnabled)
          logger.trace(
            "{} Envelope in flight, about to be processed. slice [{}], pid [{}], seqNr [{}], timestamp [{}]",
            logPrefix,
            r.record.slice,
            r.record.pid,
            r.record.seqNr,
            r.record.timestamp)
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

  def readManagementState(): Future[Option[ManagementState]] =
    dao.readManagementState(minSlice)

  def savePaused(paused: Boolean): Future[Done] =
    dao.updateManagementState(minSlice, maxSlice, paused)

}
