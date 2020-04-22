/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

import java.util.UUID

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.persistence.query
import akka.projection.ProjectionId
import akka.projection.cassandra.CassandraOffsetStore
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import com.datastax.oss.driver.api.core.cql.Row

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CassandraOffsetStoreImpl {
  private val StringManifest = "STR"
  private val LongManifest = "LNG"
  private val IntManifest = "INT"
  private val SequenceManifest = "SEQ"
  private val TimeBasedUUIDManifest = "TBU"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraOffsetStoreImpl(session: CassandraSession)(implicit ec: ExecutionContext)
    extends CassandraOffsetStore {
  import CassandraOffsetStoreImpl._

  // FIXME make keyspace and table names configurable
  val keyspace = "akka_projection"
  val table = "offset_store"

  override def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] = {
    session
      .selectOne(s"SELECT offset, manifest FROM $keyspace.$table WHERE projection_id = ?", projectionId.id)
      .map(fromRowToOffset)
  }

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): Future[Done] = {
    val (offsetStr, manifest) = stringOffsetAndManifest(offset)
    session.executeWrite(
      s"INSERT INTO $keyspace.$table (projection_id, offset, manifest) VALUES (?, ?, ?)",
      projectionId.id,
      offsetStr,
      manifest)
  }

  /**
   * Deserialize an offset from a result row.
   * The offset is converted from its string representation to its real type.
   */
  private def fromRowToOffset[Offset](maybeRow: Option[Row]): Option[Offset] = {
    maybeRow match {
      case Some(row) =>
        val offsetStr = row.getString("offset")
        val manifest = row.getString("manifest")
        val offset = (manifest match {
          case StringManifest        => offsetStr
          case LongManifest          => offsetStr.toLong
          case IntManifest           => offsetStr.toInt
          case SequenceManifest      => query.Offset.sequence(offsetStr.toLong)
          case TimeBasedUUIDManifest => query.Offset.timeBasedUUID(UUID.fromString(offsetStr))
        }).asInstanceOf[Offset]
        Some(offset)
      case None =>
        None
    }
  }

  /**
   * Convert the offset to a tuple (String, String) where the first element is
   * the String representation of the offset and the second its manifest
   */
  private def stringOffsetAndManifest[Offset](offset: Offset): (String, String) = {
    offset match {
      case s: String                => s -> StringManifest
      case l: Long                  => l.toString -> LongManifest
      case i: Int                   => i.toString -> IntManifest
      case seq: query.Sequence      => seq.value.toString -> SequenceManifest
      case tbu: query.TimeBasedUUID => tbu.value.toString -> TimeBasedUUIDManifest
      case _                        => throw new IllegalArgumentException(s"Unsupported offset type, found [${offset.getClass.getName}]")
    }
  }

  override def createKeyspaceAndTable(): Future[Done] = {
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
