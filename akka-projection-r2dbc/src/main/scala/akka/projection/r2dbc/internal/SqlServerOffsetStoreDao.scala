/*
 * Copyright (C) 2023-2024 Lightbend Inc. <https://www.lightbend.com>
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

  override protected def createSelectOneTimestampOffsetSql: String =
    sql"""
    SELECT TOP(1) seq_nr, timestamp_offset
    FROM $timestampOffsetTable WHERE slice = ? AND projection_name = ? AND persistence_id = ?
    ORDER BY seq_nr DESC"""

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

  override protected def deleteOldTimestampOffsetSql(): String = {
    s"DELETE FROM $timestampOffsetTable WHERE slice = @slice AND projection_name = @projectionName AND timestamp_offset < @timestampOffset"
  }

  override protected def bindDeleteOldTimestampOffsetSql(stmt: Statement, slice: Int, until: Instant): Statement = {

    stmt
      .bind("@slice", slice)
      .bind("@projectionName", projectionId.name)
      .bindTimestamp("@timestampOffset", until)

    stmt
  }

}
