/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

import akka.Done
import akka.annotation.InternalApi
import akka.projection.ProjectionId
import akka.projection.internal.OffsetSerialization
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraOffsetStore(session: CassandraSession)(implicit ec: ExecutionContext) {
  import OffsetSerialization.fromStorageRepresentation
  import OffsetSerialization.toStorageRepresentation

  // FIXME make keyspace and table names configurable
  val keyspace = "akka_projection"
  val table = "offset_store"

  def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] = {
    session
      .selectOne(s"SELECT offset, manifest FROM $keyspace.$table WHERE projection_id = ?", projectionId.id)
      .map { maybeRow =>
        maybeRow.map(row => fromStorageRepresentation[Offset](row.getString("offset"), row.getString("manifest")))
      }
  }

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): Future[Done] = {
    val (offsetStr, manifest) = toStorageRepresentation(offset)
    session.executeWrite(
      s"INSERT INTO $keyspace.$table (projection_id, offset, manifest) VALUES (?, ?, ?)",
      projectionId.id,
      offsetStr,
      manifest)
  }

  def createKeyspaceAndTable(): Future[Done] = {
    session
      .executeDDL(
        s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }")
      .flatMap(_ => session.executeDDL(s"""
        |CREATE TABLE IF NOT EXISTS $keyspace.$table (
        |  projection_id text,
        |  offset text,
        |  manifest text,
        |  PRIMARY KEY (projection_id))
        """.stripMargin.trim))
  }

}
