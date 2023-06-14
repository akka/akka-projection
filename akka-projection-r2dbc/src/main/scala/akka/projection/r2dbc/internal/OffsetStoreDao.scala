/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import akka.Done
import akka.annotation.InternalApi
import akka.projection.ProjectionId
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

  def readTimestampOffset(
      projectionId: ProjectionId,
      minSlice: Int,
      maxSlice: Int): Future[immutable.IndexedSeq[R2dbcOffsetStore.Record]]

  def readPrimitiveOffset(projectionId: ProjectionId): Future[immutable.IndexedSeq[OffsetSerialization.SingleOffset]]

  def insertTimestampOffsetInTx(
      connection: Connection,
      projectionId: ProjectionId,
      records: immutable.IndexedSeq[R2dbcOffsetStore.Record]): Future[Long]

  def updatePrimitiveOffsetInTx(
      connection: Connection,
      projectionId: ProjectionId,
      timestamp: Instant,
      storageRepresentation: OffsetSerialization.StorageRepresentation): Future[Done]

  def deleteOldTimestampOffset(
      projectionId: ProjectionId,
      minSlice: Int,
      maxSlice: Int,
      until: Instant,
      notInLatestBySlice: Seq[String]): Future[Long]

  def deleteNewTimestampOffsetsInTx(
      connection: Connection,
      projectionId: ProjectionId,
      minSlice: Int,
      maxSlice: Int,
      timestamp: Instant): Future[Long]

  def clearTimestampOffset(projectionId: ProjectionId, minSlice: Int, maxSlice: Int): Future[Long]

  def clearPrimitiveOffset(projectionId: ProjectionId): Future[Long]

  def readManagementState(projectionId: ProjectionId): Future[Option[ManagementState]]

  def updateManagementState(projectionId: ProjectionId, paused: Boolean, timestamp: Instant): Future[Long]

}
