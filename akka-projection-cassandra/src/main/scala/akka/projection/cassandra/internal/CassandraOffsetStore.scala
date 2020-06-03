/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

import java.time.Clock
import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.OffsetSerialization
import akka.projection.internal.OffsetSerialization.SingleOffset
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraOffsetStore(system: ActorSystem[_], clock: Clock) {
  import OffsetSerialization.fromStorageRepresentation
  import OffsetSerialization.toStorageRepresentation

  private implicit val executionContext: ExecutionContext = system.executionContext
  private val cassandraSettings = CassandraSettings(system)
  private val session: CassandraSession =
    CassandraSessionRegistry(system).sessionFor(cassandraSettings.sessionConfigPath)

  val keyspace: String = cassandraSettings.keyspace
  val table: String = cassandraSettings.table
  private val cassandraPartitions = 5

  def this(system: ActorSystem[_]) =
    this(system, Clock.systemUTC())

  def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] = {
    val partition = idToPartition(projectionId)
    session
      .selectOne(
        s"SELECT projection_key, offset, manifest FROM $keyspace.$table WHERE projection_name = ? AND partition = ? AND projection_key = ?",
        projectionId.name,
        partition,
        projectionId.key)
      .map { maybeRow =>
        maybeRow.map(row => fromStorageRepresentation[Offset](row.getString("offset"), row.getString("manifest")))
      }
  }

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): Future[Done] = {
    // a partition is calculated to ensure some distribution of projection rows across cassandra nodes, but at the
    // same time let us query all rows for a single projection_name easily
    val partition = idToPartition(projectionId)
    offset match {
      case _: MergeableOffset[_] =>
        throw new IllegalArgumentException("The CassandraOffsetStore does not currently support MergeableOffset")
      case _ =>
        val SingleOffset(_, manifest, offsetStr, _) =
          toStorageRepresentation(projectionId, offset).asInstanceOf[SingleOffset]
        session.executeWrite(
          s"INSERT INTO $keyspace.$table (projection_name, partition, projection_key, offset, manifest, last_updated) VALUES (?, ?, ?, ?, ?, ?)",
          projectionId.name,
          partition,
          projectionId.key,
          offsetStr,
          manifest,
          Instant.now(clock))
    }
  }

  // FIXME maybe we need to make this public for user's tests
  def createKeyspaceAndTable(): Future[Done] = {
    session
      .executeDDL(
        s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }")
      .flatMap(_ => session.executeDDL(s"""
        |CREATE TABLE IF NOT EXISTS $keyspace.$table (
        |  projection_name text,
        |  partition int,
        |  projection_key text,
        |  offset text,
        |  manifest text,
        |  last_updated timestamp,
        |  PRIMARY KEY ((projection_name, partition), projection_key))
        """.stripMargin.trim))
  }

  private[cassandra] def idToPartition[Offset](projectionId: ProjectionId): Integer = {
    Integer.valueOf(Math.abs(projectionId.key.hashCode() % cassandraPartitions))
  }

}
