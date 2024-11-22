/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.internal

import java.time.Instant
import java.util.Collections
import java.util.concurrent.CompletionException
import java.util.concurrent.ThreadLocalRandom
import java.util.{ HashMap => JHashMap }

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.pattern.after
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.query.TimestampOffset
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Record
import akka.projection.internal.ManagementState
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.services.dynamodb.model.Put
import software.amazon.awssdk.services.dynamodb.model.PutRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.WriteRequest
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse

/**
 * INTERNAL API
 */
@InternalApi private[projection] object OffsetStoreDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[OffsetStoreDao])

  // Hard limits in DynamoDB
  private val MaxBatchSize = 25
  private val MaxTransactItems = 100

  object OffsetStoreAttributes {
    // FIXME should attribute names be shorter?
    val Pid = "pid"
    val SeqNr = "seq_nr"
    val NameSlice = "name_slice"
    val Timestamp = "ts"
    val Seen = "seen"
    val Paused = "paused"
    val Expiry = "expiry"

    val timestampBySlicePid = AttributeValue.fromS("_")
    val managementStateBySlicePid = AttributeValue.fromS("_mgmt")
  }

  final class BatchWriteFailed(val lastResponse: BatchWriteItemResponse) extends Exception
}

/**
 * INTERNAL API
 */
@InternalApi private[projection] class OffsetStoreDao(
    system: ActorSystem[_],
    settings: DynamoDBProjectionSettings,
    projectionId: ProjectionId,
    client: DynamoDbAsyncClient) {
  import OffsetStoreDao.log
  import OffsetStoreDao.BatchWriteFailed
  import OffsetStoreDao.MaxBatchSize
  import OffsetStoreDao.MaxTransactItems
  import system.executionContext

  private val timeToLiveSettings = settings.timeToLiveSettings.projections.get(projectionId.name)

  private def nameSlice(slice: Int): String = s"${projectionId.name}-$slice"

  def loadTimestampOffset(slice: Int): Future[Option[TimestampOffset]] = {
    import OffsetStoreDao.OffsetStoreAttributes._
    val expressionAttributeValues =
      Map(":nameSlice" -> AttributeValue.fromS(nameSlice(slice)), ":pid" -> timestampBySlicePid).asJava

    val req = QueryRequest.builder
      .tableName(settings.timestampOffsetTable)
      .consistentRead(false) // not necessary to read latest, can start at earlier time
      .keyConditionExpression(s"$NameSlice = :nameSlice AND $Pid = :pid")
      .expressionAttributeValues(expressionAttributeValues)
      .projectionExpression(s"$Timestamp, $Seen")
      .build()

    client
      .query(req)
      .asScala
      .map { response =>
        val items = response.items()
        if (items.isEmpty)
          None
        else {
          val item = items.get(0)
          val timestampMicros = item.get(Timestamp).n().toLong
          val timestamp = InstantFactory.fromEpochMicros(timestampMicros)
          val seen = item.get(Seen).m().asScala.iterator.map { case (pid, attr) => pid -> attr.n().toLong }.toMap
          val timestampOffset = TimestampOffset(timestamp, seen)
          Some(timestampOffset)
        }
      }
      .recoverWith {
        case c: CompletionException =>
          Future.failed(c.getCause)
      }(ExecutionContext.parasitic)
  }

  implicit def sys: ActorSystem[_] = system

  def writeWholeBatch(batchReq: BatchWriteItemRequest): Future[List[BatchWriteItemResponse]] =
    writeWholeBatch(batchReq, settings.batchMaxRetries, settings.batchMinBackoff)

  def writeWholeBatch(
      batchReq: BatchWriteItemRequest,
      maxRetries: Int,
      backoff: FiniteDuration): Future[List[BatchWriteItemResponse]] = {
    val result = client.batchWriteItem(batchReq).asScala

    result.flatMap { response =>
      val unprocessed =
        if (response.hasUnprocessedItems && !response.unprocessedItems.isEmpty) Some(response.unprocessedItems)
        else None

      unprocessed.fold(Future.successful(List(response))) { u =>
        if (maxRetries < 1) Future.failed(new BatchWriteFailed(response))
        else {
          val newReq = batchReq.toBuilder.requestItems(u).build()
          val factor = 2.0 + ThreadLocalRandom.current().nextDouble(0.3)
          val nextDelay = {
            val clamped = (backoff * factor).min(settings.batchMaxBackoff)
            if (clamped.isFinite) clamped.toMillis.millis else settings.batchMaxBackoff
          }

          after(backoff) {
            writeWholeBatch(newReq, maxRetries - 1, nextDelay)
          }.map { responses => response :: responses }(ExecutionContext.parasitic)
        }
      }
    }
  }

  def storeTimestampOffsets(offsetsBySlice: Map[Int, TimestampOffset]): Future[Done] = {
    import OffsetStoreDao.OffsetStoreAttributes._

    val expiry = timeToLiveSettings.offsetTimeToLive.map { timeToLive =>
      Instant.now().plusSeconds(timeToLive.toSeconds)
    }

    def writeBatch(offsetsBatch: IndexedSeq[(Int, TimestampOffset)]): Future[Done] = {
      val writeItems =
        offsetsBatch.map {
          case (slice, offset) =>
            val attributes = new JHashMap[String, AttributeValue]
            attributes.put(NameSlice, AttributeValue.fromS(nameSlice(slice)))
            attributes.put(Pid, timestampBySlicePid)
            val timestampMicros = InstantFactory.toEpochMicros(offset.timestamp)
            attributes.put(Timestamp, AttributeValue.fromN(timestampMicros.toString))
            val seen = {
              if (offset.seen.isEmpty)
                Collections.emptyMap[String, AttributeValue]
              else if (offset.seen.size == 1)
                Collections.singletonMap(offset.seen.head._1, AttributeValue.fromN(offset.seen.head._2.toString))
              else {
                val seen = new JHashMap[String, AttributeValue]
                offset.seen.iterator.foreach {
                  case (pid, seqNr) => seen.put(pid, AttributeValue.fromN(seqNr.toString))
                }
                seen
              }
            }
            attributes.put(Seen, AttributeValue.fromM(seen))

            expiry.foreach { timestamp =>
              attributes.put(Expiry, AttributeValue.fromN(timestamp.getEpochSecond.toString))
            }

            WriteRequest.builder
              .putRequest(
                PutRequest
                  .builder()
                  .item(attributes)
                  .build())
              .build()
        }.asJava

      val req = BatchWriteItemRequest
        .builder()
        .requestItems(Collections.singletonMap(settings.timestampOffsetTable, writeItems))
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .build()

      val result = writeWholeBatch(req)

      if (log.isDebugEnabled()) {
        result.foreach { responses =>
          log.debug(
            "Wrote latest timestamps for [{}] slices, consumed [{}] WCU",
            offsetsBatch.size,
            responses.iterator.flatMap(_.consumedCapacity.iterator.asScala.map(_.capacityUnits.doubleValue)).sum)
        }
      }
      result
        .map(_ => Done)(ExecutionContext.parasitic)
        .recoverWith {
          case failed: BatchWriteFailed =>
            val unprocessedSliceItems = failed.lastResponse.unprocessedItems
              .get(settings.timestampOffsetTable)
              .asScala
              .toVector
              .map(_.putRequest.item)

            val unprocessedSlices = unprocessedSliceItems.map(_.get(NameSlice).s)
            log.warn(
              "Failed to write latest timestamps for [{}] slices: [{}]",
              unprocessedSlices.size,
              unprocessedSlices)

            failed.asInstanceOf[Future[Done]] // safe, actually contains Nothing

          case c: CompletionException =>
            Future.failed(c.getCause)
        }
    }

    if (offsetsBySlice.size <= MaxBatchSize) {
      writeBatch(offsetsBySlice.toVector)
    } else {
      val batches = offsetsBySlice.toVector.sliding(MaxBatchSize, MaxBatchSize)
      Future
        .sequence(batches.map(writeBatch))
        .map(_ => Done)(ExecutionContext.parasitic)
        .recoverWith {
          case c: CompletionException =>
            Future.failed(c.getCause)
        }(ExecutionContext.parasitic)
    }
  }

  def storeSequenceNumbers(records: IndexedSeq[Record]): Future[Done] = {
    val expiry = timeToLiveSettings.offsetTimeToLive.map { timeToLive =>
      Instant.now().plusSeconds(timeToLive.toSeconds)
    }

    def writeBatch(recordsBatch: IndexedSeq[Record]): Future[Done] = {
      val writeItems =
        recordsBatch
          .map { record =>
            WriteRequest.builder
              .putRequest(
                PutRequest
                  .builder()
                  .item(sequenceNumberAttributes(record, expiry))
                  .build())
              .build()
          }
          .toVector
          .asJava

      val req = BatchWriteItemRequest
        .builder()
        .requestItems(Collections.singletonMap(settings.timestampOffsetTable, writeItems))
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .build()

      val result = writeWholeBatch(req)

      if (log.isDebugEnabled()) {
        result.foreach { responses =>
          log.debug(
            "Wrote [{}] sequence numbers, consumed [{}] WCU",
            recordsBatch.size,
            responses.iterator.flatMap(_.consumedCapacity.iterator.asScala.map(_.capacityUnits.doubleValue)).sum)
        }
      }

      result
        .map(_ => Done)(ExecutionContext.parasitic)
        .recoverWith {
          case failed: BatchWriteFailed =>
            val unprocessedSeqNrItems =
              failed.lastResponse.unprocessedItems
                .get(settings.timestampOffsetTable)
                .asScala
                .toVector
                .map(_.putRequest.item)

            val unprocessedSeqNrs = unprocessedSeqNrItems.map { item =>
              import OffsetStoreDao.OffsetStoreAttributes._
              s"${item.get(NameSlice).s}: ${item.get(Pid).s}"
            }

            log.warn(
              "Failed to write sequence numbers for [{}] persistence IDs: [{}]",
              unprocessedSeqNrs.size,
              unprocessedSeqNrs.mkString(", "))

            failed.asInstanceOf[Future[Done]]

          case c: CompletionException =>
            Future.failed(c.getCause)
        }
    }

    if (records.size <= MaxBatchSize) {
      writeBatch(records)
    } else {
      val batches = records.sliding(MaxBatchSize, MaxBatchSize)
      Future
        .sequence(batches.map(writeBatch))
        .map(_ => Done)(ExecutionContext.parasitic)
    }
  }

  def loadSequenceNumber(slice: Int, pid: String): Future[Option[Record]] = {
    import OffsetStoreDao.OffsetStoreAttributes._
    val expressionAttributeValues =
      Map(":nameSlice" -> AttributeValue.fromS(nameSlice(slice)), ":pid" -> AttributeValue.fromS(pid)).asJava

    val req = QueryRequest.builder
      .tableName(settings.timestampOffsetTable)
      .consistentRead(true)
      .keyConditionExpression(s"$NameSlice = :nameSlice AND $Pid = :pid")
      .expressionAttributeValues(expressionAttributeValues)
      .projectionExpression(s"$SeqNr, $Timestamp")
      .build()

    client
      .query(req)
      .asScala
      .map { response =>
        val items = response.items()
        if (items.isEmpty)
          None
        else {
          val item = items.get(0)
          val seqNr = item.get(SeqNr).n().toLong
          val timestampMicros = item.get(Timestamp).n().toLong
          val timestamp = InstantFactory.fromEpochMicros(timestampMicros)
          Some(DynamoDBOffsetStore.Record(slice, pid, seqNr, timestamp))
        }
      }
      .recoverWith {
        case c: CompletionException =>
          Future.failed(c.getCause)
      }(ExecutionContext.parasitic)
  }

  def transactStoreSequenceNumbers(writeItems: Iterable[TransactWriteItem])(records: Seq[Record]): Future[Done] = {
    if ((writeItems.size + records.size) > MaxTransactItems)
      throw new IllegalArgumentException(
        s"Too many transactional write items. Total limit is [${MaxTransactItems}], attempting to store " +
        s"[${writeItems.size}] write items and [${records.size}] sequence numbers.")

    val expiry = timeToLiveSettings.offsetTimeToLive.map { timeToLive =>
      Instant.now().plusSeconds(timeToLive.toSeconds)
    }

    val writeSequenceNumbers = records.map { record =>
      TransactWriteItem.builder
        .put(
          Put
            .builder()
            .tableName(settings.timestampOffsetTable)
            .item(sequenceNumberAttributes(record, expiry))
            .build())
        .build()
    }

    val allTransactItems = (writeItems ++ writeSequenceNumbers).asJavaCollection

    val request = TransactWriteItemsRequest
      .builder()
      .transactItems(allTransactItems)
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()

    val result = client.transactWriteItems(request).asScala

    if (log.isDebugEnabled()) {
      result.foreach { response =>
        log.debug(
          "Atomically wrote [{}] items with [{}] sequence numbers, consumed [{}] WCU",
          writeItems.size,
          writeSequenceNumbers.size,
          response.consumedCapacity.iterator.asScala.map(_.capacityUnits.doubleValue()).sum)
      }
    }

    result
      .map(_ => Done)(ExecutionContext.parasitic)
      .recoverWith {
        case c: CompletionException =>
          Future.failed(c.getCause)
      }(ExecutionContext.parasitic)

  }

  private def sequenceNumberAttributes(record: Record, expiry: Option[Instant]): JHashMap[String, AttributeValue] = {
    import OffsetStoreDao.OffsetStoreAttributes._

    val attributes = new JHashMap[String, AttributeValue]
    attributes.put(NameSlice, AttributeValue.fromS(nameSlice(record.slice)))
    attributes.put(Pid, AttributeValue.fromS(record.pid))
    attributes.put(SeqNr, AttributeValue.fromN(record.seqNr.toString))
    val timestampMicros = InstantFactory.toEpochMicros(record.timestamp)
    attributes.put(Timestamp, AttributeValue.fromN(timestampMicros.toString))

    expiry.foreach { expiryTimestamp =>
      attributes.put(Expiry, AttributeValue.fromN(expiryTimestamp.getEpochSecond.toString))
    }

    attributes
  }

  def readManagementState(slice: Int): Future[Option[ManagementState]] = {
    import OffsetStoreDao.OffsetStoreAttributes._
    val expressionAttributeValues =
      Map(":nameSlice" -> AttributeValue.fromS(nameSlice(slice)), ":pid" -> managementStateBySlicePid).asJava

    val req = QueryRequest.builder
      .tableName(settings.timestampOffsetTable)
      .consistentRead(true)
      .keyConditionExpression(s"$NameSlice = :nameSlice AND $Pid = :pid")
      .expressionAttributeValues(expressionAttributeValues)
      .projectionExpression(s"$Paused")
      .build()

    client
      .query(req)
      .asScala
      .map { response =>
        val items = response.items()
        if (items.isEmpty)
          None
        else {
          val item = items.get(0)
          val paused =
            if (item.containsKey(Paused))
              item.get(Paused).bool().booleanValue()
            else
              false

          Some(ManagementState(paused))
        }
      }
      .recoverWith {
        case c: CompletionException =>
          Future.failed(c.getCause)
      }(ExecutionContext.parasitic)
  }

  def updateManagementState(minSlice: Int, maxSlice: Int, paused: Boolean): Future[Done] = {
    import OffsetStoreDao.OffsetStoreAttributes._

    def writeBatch(slices: Vector[Int]): Future[Done] = {
      val writeItems =
        slices.map { slice =>
          val attributes = new JHashMap[String, AttributeValue]
          attributes.put(NameSlice, AttributeValue.fromS(nameSlice(slice)))
          attributes.put(Pid, managementStateBySlicePid)
          attributes.put(Paused, AttributeValue.fromBool(paused))

          WriteRequest.builder
            .putRequest(
              PutRequest
                .builder()
                .item(attributes)
                .build())
            .build()
        }.asJava

      val req = BatchWriteItemRequest
        .builder()
        .requestItems(Collections.singletonMap(settings.timestampOffsetTable, writeItems))
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .build()

      val result = writeWholeBatch(req)

      if (log.isDebugEnabled()) {
        result.foreach { responses =>
          log.debug(
            "Wrote management state for [{}] slices, consumed [{}] WCU",
            slices.size,
            responses.iterator.flatMap(_.consumedCapacity.iterator.asScala.map(_.capacityUnits.doubleValue)).sum)
        }
      }
      result
        .map(_ => Done)(ExecutionContext.parasitic)
        .recoverWith {
          case failed: BatchWriteFailed =>
            val unprocessedStateItems =
              failed.lastResponse.unprocessedItems
                .get(settings.timestampOffsetTable)
                .asScala
                .toVector
                .map(_.putRequest.item)

            val unprocessedStates = unprocessedStateItems.map { item =>
              s"${item.get(NameSlice).s}-${item.get(Paused).bool}"
            }

            log.warn(
              "Failed to write management state for [{}] slices: [{}]",
              unprocessedStates.size,
              unprocessedStates.mkString(", "))

            failed.asInstanceOf[Future[Done]]

          case c: CompletionException =>
            Future.failed(c.getCause)
        }
    }

    val sliceRange = (minSlice to maxSlice).toVector
    if (sliceRange.size <= MaxBatchSize) {
      writeBatch(sliceRange)
    } else {
      val batches = sliceRange.sliding(MaxBatchSize, MaxBatchSize)
      Future
        .sequence(batches.map(writeBatch))
        .map(_ => Done)(ExecutionContext.parasitic)
    }
  }
}
