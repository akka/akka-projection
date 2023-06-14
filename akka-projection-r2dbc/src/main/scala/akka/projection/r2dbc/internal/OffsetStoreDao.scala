/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
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

/**
 * INTERNAL API
 */
@InternalApi
private[projection] trait OffsetStoreDao {

  def readTimestampOffset(minSlice: Int, maxSlice: Int): Future[immutable.IndexedSeq[R2dbcOffsetStore.Record]]

  def readPrimitiveOffset(): Future[immutable.IndexedSeq[OffsetSerialization.SingleOffset]]

  def insertTimestampOffsetInTx(
      connection: Connection,
      records: immutable.IndexedSeq[R2dbcOffsetStore.Record]): Future[Long]

  def updatePrimitiveOffsetInTx(
      connection: Connection,
      timestamp: Instant,
      storageRepresentation: OffsetSerialization.StorageRepresentation): Future[Done]

  def deleteOldTimestampOffset(
      minSlice: Int,
      maxSlice: Int,
      until: Instant,
      notInLatestBySlice: Seq[String]): Future[Long]

  def deleteNewTimestampOffsetsInTx(
      connection: Connection,
      minSlice: Int,
      maxSlice: Int,
      timestamp: Instant): Future[Long]

  def clearTimestampOffset(minSlice: Int, maxSlice: Int): Future[Long]

  def clearPrimitiveOffset(): Future[Long]

  def readManagementState(): Future[Option[ManagementState]]

  def updateManagementState(paused: Boolean, timestamp: Instant): Future[Long]

}
