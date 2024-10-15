/*
 * Copyright (C) 2023-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import java.time.Instant

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import io.r2dbc.spi.Connection
import io.r2dbc.spi.Statement
import org.slf4j.LoggerFactory

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.IdentityAdapter
import akka.persistence.r2dbc.internal.codec.QueryAdapter
import akka.persistence.r2dbc.internal.codec.TimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.PostgresTimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichRow
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.internal.OffsetSerialization
import akka.projection.internal.OffsetSerialization.MultipleOffsets
import akka.projection.internal.OffsetSerialization.SingleOffset
import akka.projection.internal.OffsetSerialization.StorageRepresentation
import akka.projection.r2dbc.R2dbcProjectionSettings
import akka.projection.r2dbc.internal.R2dbcOffsetStore.LatestBySlice
import akka.projection.r2dbc.internal.R2dbcOffsetStore.Record

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class PostgresOffsetStoreDao(
    settings: R2dbcProjectionSettings,
    sourceProvider: Option[BySlicesSourceProvider],
    system: ActorSystem[_],
    r2dbcExecutor: R2dbcExecutor,
    projectionId: ProjectionId)
    extends OffsetStoreDao {

  private val logger = LoggerFactory.getLogger(getClass)

  private val persistenceExt = Persistence(system)

  protected val timestampOffsetTable: String = settings.timestampOffsetTableWithSchema
  protected val offsetTable: String = settings.offsetTableWithSchema
  protected val managementTable: String = settings.managementTableWithSchema

  protected implicit def queryAdapter: QueryAdapter = IdentityAdapter
  protected implicit def timestampCodec: TimestampCodec = PostgresTimestampCodec

  private implicit val ec: ExecutionContext = system.executionContext

  private val selectTimestampOffsetSql: String =
    sql"""
    SELECT projection_key, slice, persistence_id, seq_nr, timestamp_offset
    FROM $timestampOffsetTable WHERE slice BETWEEN ? AND ? AND projection_name = ?"""

  private val insertTimestampOffsetSql: String =
    sql"""
    INSERT INTO $timestampOffsetTable
    (projection_name, projection_key, slice, persistence_id, seq_nr, timestamp_offset, timestamp_consumed)
    VALUES (?,?,?,?,?,?, CURRENT_TIMESTAMP)"""

  private val insertTimestampOffsetBatchSql: String = {
    val values = (1 to settings.offsetBatchSize).map(_ => "(?,?,?,?,?,?, CURRENT_TIMESTAMP)").mkString(", ")
    sql"""
    INSERT INTO $timestampOffsetTable
    (projection_name, projection_key, slice, persistence_id, seq_nr, timestamp_offset, timestamp_consumed)
    VALUES $values
    """
  }

  /**
   * delete less than a timestamp
   * @param notInLatestBySlice not used in postgres, but needed in sql
   */
  @nowarn
  protected def deleteOldTimestampOffsetSql(notInLatestBySlice: Seq[LatestBySlice]): String =
    sql"""
    DELETE FROM $timestampOffsetTable WHERE slice BETWEEN ? AND ? AND projection_name = ? AND timestamp_offset < ?
    AND NOT (persistence_id || '-' || seq_nr) = ANY (?)"""

  protected def bindDeleteOldTimestampOffsetSql(
      stmt: Statement,
      minSlice: Int,
      maxSlice: Int,
      until: Instant,
      notInLatestBySlice: Seq[LatestBySlice]): Statement = {
    stmt
      .bind(0, minSlice)
      .bind(1, maxSlice)
      .bind(2, projectionId.name)
      .bindTimestamp(3, until)
      .bind(4, notInLatestBySlice.iterator.map(record => s"${record.pid}-${record.seqNr}").toArray[String])
  }

  // delete greater than or equal a timestamp
  private val deleteNewTimestampOffsetSql: String =
    sql"DELETE FROM $timestampOffsetTable WHERE slice BETWEEN ? AND ? AND projection_name = ? AND timestamp_offset >= ?"

  private val clearTimestampOffsetSql: String =
    sql"DELETE FROM $timestampOffsetTable WHERE slice BETWEEN ? AND ? AND projection_name = ?"

  private val selectOffsetSql: String =
    sql"SELECT projection_key, current_offset, manifest, mergeable FROM $offsetTable WHERE projection_name = ?"

  protected def createUpsertOffsetSql(): String =
    sql"""
    INSERT INTO $offsetTable
    (projection_name, projection_key, current_offset, manifest, mergeable, last_updated)
    VALUES (?,?,?,?,?,?)
    ON CONFLICT (projection_name, projection_key)
    DO UPDATE SET
    current_offset = excluded.current_offset,
    manifest = excluded.manifest,
    mergeable = excluded.mergeable,
    last_updated = excluded.last_updated"""

  private val upsertOffsetSql: String = createUpsertOffsetSql()

  private val clearOffsetSql: String =
    sql"DELETE FROM $offsetTable WHERE projection_name = ? AND projection_key = ?"

  private val readManagementStateSql =
    sql"""
    SELECT paused FROM $managementTable WHERE
    projection_name = ? AND
    projection_key = ? """

  protected def createUpdateManagementStateSql(): String =
    sql"""
      INSERT INTO $managementTable
      (projection_name, projection_key, paused, last_updated)
      VALUES (?,?,?,?)
      ON CONFLICT (projection_name, projection_key)
      DO UPDATE SET
      paused = excluded.paused,
      last_updated = excluded.last_updated"""

  protected def bindCreateUpdateManagementStateSql(
      stmt: Statement,
      projectionId: ProjectionId,
      paused: Boolean,
      lastUpdated: Long): Statement = {
    bindUpdateManagementStateSql(stmt, projectionId, paused, lastUpdated)
  }

  protected def bindUpdateManagementStateSql(
      stmt: Statement,
      projectionId: ProjectionId,
      paused: Boolean,
      lastUpdated: Long): Statement = {
    stmt
      .bind(0, projectionId.name)
      .bind(1, projectionId.key)
      .bind(2, paused)
      .bind(3, lastUpdated)
  }

  protected def timestampOffsetBySlicesSourceProvider: BySlicesSourceProvider =
    sourceProvider match {
      case Some(provider) => provider
      case None =>
        throw new IllegalArgumentException(
          s"Expected BySlicesSourceProvider to be defined when TimestampOffset is used.")
    }

  override def readTimestampOffset(): Future[immutable.IndexedSeq[R2dbcOffsetStore.RecordWithProjectionKey]] = {
    val (minSlice, maxSlice) = {
      sourceProvider match {
        case Some(provider) => (provider.minSlice, provider.maxSlice)
        case None           => (0, persistenceExt.numberOfSlices - 1)
      }
    }
    r2dbcExecutor.select("read timestamp offset")(
      conn => {
        logger.trace("reading timestamp offset for [{}]", projectionId)
        conn
          .createStatement(selectTimestampOffsetSql)
          .bind(0, minSlice)
          .bind(1, maxSlice)
          .bind(2, projectionId.name)
      },
      row => {
        val projectionKey = row.get("projection_key", classOf[String])
        val slice = row.get("slice", classOf[java.lang.Integer])
        val pid = row.get("persistence_id", classOf[String])
        val seqNr = row.get("seq_nr", classOf[java.lang.Long])
        val timestamp = row.getTimestamp("timestamp_offset")
        R2dbcOffsetStore.RecordWithProjectionKey(R2dbcOffsetStore.Record(slice, pid, seqNr, timestamp), projectionKey)
      })
  }

  override def readPrimitiveOffset(): Future[immutable.IndexedSeq[OffsetSerialization.SingleOffset]] =
    r2dbcExecutor.select("read offset")(
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
        OffsetSerialization.SingleOffset(adaptedProjectionId, manifest, offsetStr, mergeable)
      })

  override def insertTimestampOffsetInTx(
      connection: Connection,
      records: immutable.IndexedSeq[R2dbcOffsetStore.Record]): Future[Long] = {
    def bindRecord(stmt: Statement, record: Record, bindStartIndex: Int): Statement = {
      val slice = persistenceExt.sliceForPersistenceId(record.pid)
      val minSlice = timestampOffsetBySlicesSourceProvider.minSlice
      val maxSlice = timestampOffsetBySlicesSourceProvider.maxSlice
      if (slice < minSlice || slice > maxSlice)
        throw new IllegalArgumentException(
          s"This offset store [$projectionId] manages slices " +
          s"[$minSlice - $maxSlice] but received slice [$slice] for persistenceId [${record.pid}]")

      stmt
        .bind(bindStartIndex, projectionId.name)
        .bind(bindStartIndex + 1, projectionId.key)
        .bind(bindStartIndex + 2, slice)
        .bind(bindStartIndex + 3, record.pid)
        .bind(bindStartIndex + 4, record.seqNr)
        .bindTimestamp(bindStartIndex + 5, record.timestamp)
    }

    require(records.nonEmpty)

    logger.trace("saving timestamp offset [{}], {}", records.last.timestamp, records)

    if (records.size == 1) {
      val statement = connection.createStatement(insertTimestampOffsetSql)
      val boundStatement = bindRecord(statement, records.head, bindStartIndex = 0)
      R2dbcExecutor.updateOneInTx(boundStatement)
    } else {
      val batchSize = settings.offsetBatchSize
      val batches = if (batchSize > 0) records.size / batchSize else 0
      val batchResult =
        if (batches > 0) {
          val batchStatements =
            (0 until batches).map { i =>
              val stmt = connection.createStatement(insertTimestampOffsetBatchSql)
              records.slice(i * batchSize, i * batchSize + batchSize).zipWithIndex.foreach {
                case (rec, recIdx) =>
                  bindRecord(stmt, rec, recIdx * 6) // 6 bind parameters per record
              }
              stmt
            }
          R2dbcExecutor.updateInTx(batchStatements).map(_.sum)
        } else
          Future.successful(0L)

      batchResult.flatMap { batchResultCount =>
        val remainingRecords = records.drop(batches * batchSize)
        if (remainingRecords.nonEmpty) {
          val statement = connection.createStatement(insertTimestampOffsetSql)
          val boundStatement =
            remainingRecords.foldLeft(statement) { (stmt, rec) =>
              stmt.add()
              bindRecord(stmt, rec, bindStartIndex = 0)
            }
          // This "batch" statement is not efficient, see issue #897
          R2dbcExecutor
            .updateBatchInTx(boundStatement)
            .map(_ + batchResultCount)(ExecutionContext.parasitic)
        } else
          Future.successful(batchResultCount)
      }
    }
  }

  protected def bindUpsertOffsetSql(stmt: Statement, singleOffset: SingleOffset, toEpochMilli: Long): Statement = {
    stmt
      .bind(0, singleOffset.id.name)
      .bind(1, singleOffset.id.key)
      .bind(2, singleOffset.offsetStr)
      .bind(3, singleOffset.manifest)
      .bind(4, java.lang.Boolean.valueOf(singleOffset.mergeable))
      .bind(5, toEpochMilli)
  }

  override def updatePrimitiveOffsetInTx(
      connection: Connection,
      timestamp: Instant,
      storageRepresentation: StorageRepresentation): Future[Done] = {
    def upsertStmt(singleOffset: SingleOffset): Statement = {
      val stmt = connection.createStatement(upsertOffsetSql)
      bindUpsertOffsetSql(stmt, singleOffset, timestamp.toEpochMilli)
    }

    val statements = storageRepresentation match {
      case single: SingleOffset  => Vector(upsertStmt(single))
      case MultipleOffsets(many) => many.map(upsertStmt).toVector
    }

    R2dbcExecutor.updateInTx(statements).map(_ => Done)(ExecutionContext.parasitic)
  }

  override def deleteOldTimestampOffset(until: Instant, notInLatestBySlice: Seq[LatestBySlice]): Future[Long] = {
    val minSlice = timestampOffsetBySlicesSourceProvider.minSlice
    val maxSlice = timestampOffsetBySlicesSourceProvider.maxSlice
    r2dbcExecutor.updateOne("delete old timestamp offset") { conn =>
      val stmt = conn.createStatement(deleteOldTimestampOffsetSql(notInLatestBySlice))
      bindDeleteOldTimestampOffsetSql(stmt, minSlice, maxSlice, until, notInLatestBySlice)
    }
  }

  override def deleteNewTimestampOffsetsInTx(connection: Connection, timestamp: Instant): Future[Long] = {
    val minSlice = timestampOffsetBySlicesSourceProvider.minSlice
    val maxSlice = timestampOffsetBySlicesSourceProvider.maxSlice
    R2dbcExecutor.updateOneInTx(
      connection
        .createStatement(deleteNewTimestampOffsetSql)
        .bind(0, minSlice)
        .bind(1, maxSlice)
        .bind(2, projectionId.name)
        .bindTimestamp(3, timestamp))
  }

  override def clearTimestampOffset(): Future[Long] = {
    val minSlice = timestampOffsetBySlicesSourceProvider.minSlice
    val maxSlice = timestampOffsetBySlicesSourceProvider.maxSlice
    r2dbcExecutor
      .updateOne("clear timestamp offset") { conn =>
        logger.debug("clearing timestamp offset for [{}]", projectionId)
        conn
          .createStatement(clearTimestampOffsetSql)
          .bind(0, minSlice)
          .bind(1, maxSlice)
          .bind(2, projectionId.name)
      }
  }

  override def clearPrimitiveOffset(): Future[Long] =
    r2dbcExecutor
      .updateOne("clear offset") { conn =>
        logger.debug("clearing offset for [{}]", projectionId)
        conn
          .createStatement(clearOffsetSql)
          .bind(0, projectionId.name)
          .bind(1, projectionId.key)
      }

  override def readManagementState(): Future[Option[ManagementState]] = {
    r2dbcExecutor
      .selectOne("read management state")(
        _.createStatement(readManagementStateSql)
          .bind(0, projectionId.name)
          .bind(1, projectionId.key),
        row => ManagementState(row.get[java.lang.Boolean]("paused", classOf[java.lang.Boolean])))
  }

  override def updateManagementState(paused: Boolean, timestamp: Instant): Future[Long] =
    r2dbcExecutor
      .updateOne("update management state") { conn =>
        val stmt = conn.createStatement(createUpdateManagementStateSql())
        bindCreateUpdateManagementStateSql(stmt, projectionId, paused, timestamp.toEpochMilli)
      }
}
