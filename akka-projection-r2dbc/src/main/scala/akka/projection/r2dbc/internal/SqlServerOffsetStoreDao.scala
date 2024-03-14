/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import java.time.Instant

import io.r2dbc.spi.Statement

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.codec.QueryAdapter
import akka.persistence.r2dbc.internal.codec.SqlServerQueryAdapter
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.TimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.SqlServerTimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.projection.r2dbc.R2dbcProjectionSettings
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.internal.OffsetSerialization.SingleOffset
import akka.projection.r2dbc.internal.R2dbcOffsetStore.LatestBySlice

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class SqlServerOffsetStoreDao(
    settings: R2dbcProjectionSettings,
    sourceProvider: Option[BySlicesSourceProvider],
    system: ActorSystem[_],
    r2dbcExecutor: R2dbcExecutor,
    projectionId: ProjectionId)
    extends PostgresOffsetStoreDao(settings, sourceProvider, system, r2dbcExecutor, projectionId) {

  override protected implicit def queryAdapter: QueryAdapter = SqlServerQueryAdapter

  override protected implicit def timestampCodec: TimestampCodec = SqlServerTimestampCodec

  override protected def createUpsertOffsetSql() =
    sql"""
            UPDATE $offsetTable SET
              current_offset = @currentOffset,
              manifest = @manifest,
              mergeable = @mergeable,
              last_updated = @lastUpdated
            WHERE projection_name = @projectionName AND projection_key = @projectionKey
            if @@ROWCOUNT = 0
              INSERT INTO $offsetTable
                (projection_name, projection_key, current_offset, manifest, mergeable, last_updated)
                VALUES (@projectionName, @projectionKey, @currentOffset, @manifest, @mergeable, @lastUpdated)
              """

  override protected def bindUpsertOffsetSql(
      stmt: Statement,
      singleOffset: SingleOffset,
      toEpochMilli: Long): Statement = {
    stmt
      .bind("@projectionName", singleOffset.id.name)
      .bind("@projectionKey", singleOffset.id.key)
      .bind("@currentOffset", singleOffset.offsetStr)
      .bind("@manifest", singleOffset.manifest)
      .bind("@mergeable", java.lang.Boolean.valueOf(singleOffset.mergeable))
      .bind("@lastUpdated", toEpochMilli)
  }

  override protected def createUpdateManagementStateSql(): String = {
    sql"""
         UPDATE $managementTable SET
           paused = @paused,
           last_updated = @lastUpdated
         WHERE projection_name = @projectionName AND projection_key = @projectionKey
         if @@ROWCOUNT = 0
           INSERT INTO $managementTable
           (projection_name, projection_key, paused, last_updated)
           VALUES (@projectionName, @projectionKey, @paused, @lastUpdated)
         """
  }

  override protected def bindUpdateManagementStateSql(
      stmt: Statement,
      projectionId: ProjectionId,
      paused: Boolean,
      lastUpdated: Long): Statement = {
    stmt
      .bind("@paused", paused)
      .bind("@lastUpdated", lastUpdated)
      .bind("@projectionName", projectionId.name)
      .bind("@projectionKey", projectionId.key)
  }

  /**
   * The r2dbc-sqlserver driver seems to not support binding of array[T].
   * So have to bake the param into the statement instead of binding it.
   *
   * @param notInLatestBySlice not used in postgres, but needed in sql
   * @return
   */
  override protected def deleteOldTimestampOffsetSql(notInLatestBySlice: Seq[LatestBySlice]): String = {
    val base =
      s"DELETE FROM $timestampOffsetTable WHERE slice BETWEEN @from AND @to AND projection_name = @projectionName AND timestamp_offset < @timestampOffset"
    if (notInLatestBySlice.isEmpty) {
      sql"$base"
    } else {

      val values = (timestampOffsetBySlicesSourceProvider.minSlice to timestampOffsetBySlicesSourceProvider.maxSlice)
        .map { i =>
          s"@s$i"
        }
        .mkString(", ")
      sql"""
        $base
        AND CONCAT(persistence_id, '-', seq_nr) NOT IN ($values)"""
    }
  }

  override protected def bindDeleteOldTimestampOffsetSql(
      stmt: Statement,
      minSlice: Int,
      maxSlice: Int,
      until: Instant,
      notInLatestBySlice: Seq[LatestBySlice]): Statement = {

    stmt
      .bind("@from", minSlice)
      .bind("@to", maxSlice)
      .bind("@projectionName", projectionId.name)
      .bindTimestamp("@timestampOffset", until)

    if (notInLatestBySlice.nonEmpty) {
      val sliceLookup = notInLatestBySlice.map { item =>
        item.slice -> item
      }.toMap

      (timestampOffsetBySlicesSourceProvider.minSlice to timestampOffsetBySlicesSourceProvider.maxSlice).foreach { i =>
        val bindKey = s"@s$i"
        sliceLookup.get(i) match {
          case Some(value) => stmt.bind(bindKey, s"${value.pid}-${value.seqNr}")
          case None        => stmt.bind(bindKey, "-")
        }
      }
    }

    stmt
  }

}
