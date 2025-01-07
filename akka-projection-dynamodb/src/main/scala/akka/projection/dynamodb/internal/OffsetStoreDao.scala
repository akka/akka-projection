/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.internal

import java.time.Instant
import java.util.Collections
import java.util.concurrent.CompletionException
import java.util.{ HashMap => JHashMap }

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.query.TimestampOffset
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Record
import akka.projection.dynamodb.scaladsl.Retry
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

  final class BatchWriteFailed(val lastResponse: BatchWriteItemResponse) extends RuntimeException
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
  import OffsetStoreDao.MaxTransactItems
  import settings.offsetBatchSize
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

  private def writeBatchWithRetries(request: BatchWriteItemRequest): Future[Seq[BatchWriteItemResponse]] =
    Retry.batchWrite(
      client,
      request,
      settings.retrySettings.maxRetries,
      settings.retrySettings.minBackoff,
      settings.retrySettings.maxBackoff,
      settings.retrySettings.randomFactor,
      onRetry = (response, retry, delay) =>
        if (log.isDebugEnabled) {
          val count = response.unprocessedItems.asScala.valuesIterator.map(_.size).sum
          log.debug(
            "Not all writes in batch were applied, retrying in [{} ms]: [{}] unapplied writes, [{}/{}] retries",
            delay.toMillis,
            count,
            retry,
            settings.retrySettings.maxRetries)
        },
      failOnMaxRetries = new BatchWriteFailed(_))(system)

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

      val result = writeBatchWithRetries(req)

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
            val unprocessedSlices = failed.lastResponse.unprocessedItems
              .get(settings.timestampOffsetTable)
              .iterator
              .asScala
              .map { req =>
                val item = req.putRequest.item
                item.get(NameSlice).s
              }
              .toVector

            log.warn(
              "Failed to write latest timestamps for [{}] slices: [{}]",
              unprocessedSlices.size,
              unprocessedSlices.mkString(", "))

            Future.failed(failed)

          case c: CompletionException =>
            Future.failed(c.getCause)
        }
    }

    if (offsetsBySlice.size <= offsetBatchSize) {
      writeBatch(offsetsBySlice.toVector)
    } else {
      val batches = offsetsBySlice.toVector.sliding(offsetBatchSize, offsetBatchSize)
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

      val result = writeBatchWithRetries(req)

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
            val unprocessedSeqNrs =
              failed.lastResponse.unprocessedItems
                .get(settings.timestampOffsetTable)
                .iterator
                .asScala
                .map { req =>
                  val item = req.putRequest.item
                  import OffsetStoreDao.OffsetStoreAttributes._
                  s"${item.get(NameSlice).s}: ${item.get(Pid).s}"
                }
                .toVector

            log.warn(
              "Failed to write sequence numbers for [{}] persistence IDs: [{}]",
              unprocessedSeqNrs.size,
              unprocessedSeqNrs.mkString(", "))

            Future.failed(failed)

          case c: CompletionException =>
            Future.failed(c.getCause)
        }
    }

    if (records.size <= offsetBatchSize) {
      writeBatch(records)
    } else {
      val batches = records.sliding(offsetBatchSize, offsetBatchSize)
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
        s"Too many transactional write items. Total limit is [$MaxTransactItems], attempting to store " +
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

      val result = writeBatchWithRetries(req)

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
            val unprocessedStates =
              failed.lastResponse.unprocessedItems
                .get(settings.timestampOffsetTable)
                .iterator
                .asScala
                .map { req =>
                  val item = req.putRequest.item
                  s"${item.get(NameSlice).s}-${item.get(Paused).bool}"
                }
                .toVector

            log.warn(
              "Failed to write management state for [{}] slices: [{}]",
              unprocessedStates.size,
              unprocessedStates.mkString(", "))

            Future.failed(failed)

          case c: CompletionException =>
            Future.failed(c.getCause)
        }
    }

    val sliceRange = (minSlice to maxSlice).toVector
    if (sliceRange.size <= offsetBatchSize) {
      writeBatch(sliceRange)
    } else {
      val batches = sliceRange.sliding(offsetBatchSize, offsetBatchSize)
      Future
        .sequence(batches.map(writeBatch))
        .map(_ => Done)(ExecutionContext.parasitic)
    }
  }
}
