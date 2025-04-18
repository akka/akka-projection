/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import akka.Done
import akka.annotation.InternalApi
import akka.projection.internal.ManagementState
import akka.projection.internal.OffsetSerialization
import io.r2dbc.spi.Connection
import java.time.Instant

import scala.collection.immutable
import scala.concurrent.Future

import akka.projection.r2dbc.internal.R2dbcOffsetStore.LatestBySlice

/**
 * INTERNAL API
 */
@InternalApi
private[projection] trait OffsetStoreDao {

  def readTimestampOffset(slice: Int): Future[immutable.IndexedSeq[R2dbcOffsetStore.RecordWithProjectionKey]]

  def readTimestampOffset(slice: Int, pid: String): Future[Option[R2dbcOffsetStore.Record]]

  def readPrimitiveOffset(): Future[immutable.IndexedSeq[OffsetSerialization.SingleOffset]]

  def insertTimestampOffsetInTx(
      connection: Connection,
      records: immutable.IndexedSeq[R2dbcOffsetStore.Record]): Future[Long]

  def updatePrimitiveOffsetInTx(
      connection: Connection,
      timestamp: Instant,
      storageRepresentation: OffsetSerialization.StorageRepresentation): Future[Done]

  def deleteOldTimestampOffset(slice: Int, until: Instant): Future[Long]

  def deleteNewTimestampOffsetsInTx(connection: Connection, timestamp: Instant): Future[Long]

  def adoptTimestampOffsets(latestBySlice: Seq[LatestBySlice]): Future[Long]

  def clearTimestampOffset(): Future[Long]

  def clearPrimitiveOffset(): Future[Long]

  def readManagementState(): Future[Option[ManagementState]]

  def updateManagementState(paused: Boolean, timestamp: Instant): Future[Long]

}
