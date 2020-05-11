/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

import java.time.Clock
import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.Done
import akka.annotation.InternalApi
import akka.projection.ProjectionId
import akka.projection.internal.{ MergeableOffset, OffsetSerialization }
import akka.projection.internal.OffsetSerialization.SingleOffset
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraOffsetStore(session: CassandraSession, clock: Clock)(
    implicit ec: ExecutionContext) {
  import OffsetSerialization.fromStorageRepresentation
  import OffsetSerialization.toStorageRepresentation

  // FIXME make keyspace and table names configurable
  val keyspace = "akka_projection"
  val table = "offset_store"

  def this(session: CassandraSession)(implicit ec: ExecutionContext) =
    this(session, Clock.systemUTC())

  def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] = {
    session
      .selectOne(s"SELECT offset, manifest FROM $keyspace.$table WHERE projection_id = ?", projectionId.id)
      .map { maybeRow =>
        maybeRow.map(row => fromStorageRepresentation[Offset](row.getString("offset"), row.getString("manifest")))
      }
  }

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): Future[Done] =
    offset match {
      case _: MergeableOffset[_] =>
        throw new IllegalArgumentException("The CassandraOffsetStore does not currently support MergeableOffset")
      case _ =>
        val SingleOffset(_, manifest, offsetStr, _) =
          toStorageRepresentation(projectionId, offset).asInstanceOf[SingleOffset]
        session.executeWrite(
          s"INSERT INTO $keyspace.$table (projection_id, offset, manifest, last_updated) VALUES (?, ?, ?, ?)",
          projectionId.id,
          offsetStr,
          manifest,
          Instant.now(clock))
    }

  // FIXME maybe we need to make this public for user's tests
  def createKeyspaceAndTable(): Future[Done] = {
    session
      .executeDDL(
        s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }")
      .flatMap(_ => session.executeDDL(s"""
        |CREATE TABLE IF NOT EXISTS $keyspace.$table (
        |  projection_id text,
        |  offset text,
        |  manifest text,
        |  last_updated timestamp,
        |  PRIMARY KEY (projection_id))
        """.stripMargin.trim))
  }

}
